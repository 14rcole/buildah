package imagebuildah

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	is "github.com/containers/image/storage"
	"github.com/containers/storage"
	"github.com/docker/docker/builder/dockerfile/parser"
	"github.com/fatih/structs"
	"github.com/openshift/imagebuilder"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// Dependency map for parallelization
var depMap = map[string][]string{
	"from":        []string{},
	"run":         []string{"add", "copy", "env", "run", "user", "workdir"},
	"cmd":         []string{},
	"label":       []string{},
	"maintainer":  []string{},
	"expose":      []string{},
	"add":         []string{}, // add and copy are not dependent on run because these
	"copy":        []string{}, // commands will make intermediate dirs if need be
	"entrypoint":  []string{},
	"volume":      []string{},
	"user":        []string{"cmd", "entrypoint", "run", "shell"},
	"workdir":     []string{"add", "cmd", "copy", "entrypoint", "run", "shell"},
	"env":         []string{}, // this may actually create conditional dependencies
	"arg":         []string{}, // similarly may create conditional deps for RUN command
	"onbuild":     []string{},
	"stopsignal":  []string{},
	"healthcheck": []string{},
	"shell":       []string{},
}

// DepNode contains information about a node's prerequisites
type DepNode struct {
	command string
	node    *parser.Node
	prereqs []int
}

type BuildRequest struct {
	Success bool   `json:"success"` // if this is true, error will be "", else, diff will be ""
	Diff    []byte `json:"diff"`
	Error   string `json:"error"`
}

type ParallelBuilder struct {
	Builder  *imagebuilder.Builder `json:"builder"`
	Options  BuildOptions          `json:"options"`
	From     string                `json:"from"`
	Node     *parser.Node          `json:"node"`
	Diff     []byte                `json:"diff"`
	workers  []string
	busy     []int
	complete []bool
	executor *Executor
}

// MarshalJSON converts a ParallelBuilder into a json []byte.  This must be
// done (partially) manually, as ParallelBuilder.Options includes a func()
// parameter that cannot be parsed by json.Marshal
func (b *ParallelBuilder) MarshalJSON() ([]byte, error) {
	s := structs.New(b)
	buildMap := s.Map()
	optMap := buildMap["Options"].(map[string]interface{})
	delete(optMap, "Log")
	return json.Marshal(buildMap)
}

// MarshalIndentJSON does the same thing as MarshalJson, formatted for readability
func (b *ParallelBuilder) MarshalIndentJSON() ([]byte, error) {
	s := structs.New(b)
	buildMap := s.Map()
	optMap := buildMap["Options"].(map[string]interface{})
	delete(optMap, "Log")
	return json.MarshalIndent(buildMap, "", "  ")
}

// BuildDepTree builds a dependency tree for a set of commands to facilitate parallel
// execution
func BuildDepTree(node *parser.Node) []*DepNode {
	fmt.Printf("Building %d nodes\n", len(node.Children))
	tree := make([]*DepNode, len(node.Children))
	for i := range tree {
		tree[i] = new(DepNode)
	}
	// The head node is just a placeholder
	for i, child := range node.Children {
		tree[i] = &DepNode{
			command: child.Value,
			node:    child,
			prereqs: getPrereqs(node, tree),
		}
	}
	return tree
}

func getPrereqs(current *parser.Node, prev []*DepNode) []int {
	//Keep this just in case: prereqs := make([]int, 0)
	var prereqs []int
	for i := range prev {
		if isDependent(current.Value, prev[i].command) {
			prereqs = append(prereqs, i)
		}
	}
	return prereqs
}

func isDependent(cmd1, cmd2 string) bool {
	possibleDeps := depMap[cmd1]
	for _, pd := range possibleDeps {
		if pd == cmd2 {
			return true
		}
	}
	return false
}

// SetWorkers sets the list of worker nodes on which parallel builds are executed
func (b *ParallelBuilder) SetWorkers(w []string) {
	fmt.Println("workers: ", w)
	b.workers = w
	b.busy = make([]int, len(b.workers))
}

func (b *ParallelBuilder) BuildParallel(ib *imagebuilder.Builder, node []*parser.Node) (err error) {
	if len(node) == 0 {
		return errors.Wrapf(err, "error building: no build instructions")
	}
	if err != nil {
		return err
	}
	first := node[0]
	from, err := ib.From(first)
	if err != nil {
		return err
	}
	b.From = from
	if err = b.pullRootImage(ib, first); err != nil {
		return err
	}
	if from, err = b.createEmptyLayer(ib, ""); err != nil {
		return err
	}
	if err != nil {
		logrus.Debugf("Build(first.Children=%+v)", first.Children)
		return errors.Wrapf(err, "error determining starting point for build")
	}

	depNodes := BuildDepTree(node[0])
	b.complete = make([]bool, len(depNodes))
	var wg sync.WaitGroup
	wg.Add(len(depNodes))
	for i, this := range depNodes {
		go func(b *ParallelBuilder, step int, this *DepNode, wg *sync.WaitGroup) {
			b.waitForPrereqs(this)
			worker := b.findNotBusy()
			err = b.BuildAndMerge(ib, this, from, step, worker)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Could not build step %q: %s\n", this.node.Original, err)
				os.Exit(1)
			}
			b.complete[step] = true
			wg.Done()
		}(b, i, this, &wg)
	}
	wg.Wait()
	if err = b.Commit(from); err != nil {
		return err
	}
	return nil
}

func (b *ParallelBuilder) waitForPrereqs(node *DepNode) {
	for len(node.prereqs) > 0 {
		deleted := 0
		for i := range node.prereqs {
			j := i - deleted
			if b.complete[node.prereqs[i]] {
				node.prereqs = node.prereqs[:j+copy(node.prereqs[j:], node.prereqs[j+1:])]
				deleted++
			}
		}
	}
}

func (b *ParallelBuilder) findNotBusy() int {
	for true {
		for i := 0; i < len(b.busy); i++ {
			if b.busy[i] == 0 {
				b.busy[i] = 1
				return i
			}
		}
	}
	return 0
}

func (b *ParallelBuilder) BuildAndMerge(ib *imagebuilder.Builder, node *DepNode, from string, step int, worker int) error {
	fmt.Println("building...")
	stripNode(node.node)
	// Wait until prerequs are completed
	// Send request to worker to build the step
	if strings.EqualFold(node.node.Value, "COPY") {
		diff, err := b.BuildLocal()
		if err != nil {
			return err
		}
		return b.Merge(ib, from, diff)
	} else {
		body, err := b.sendRequest(worker)
		b.busy[worker] = 0
		if err != nil {
			return err
		}
		fmt.Println("completed request")
		if !body.Success {
			fmt.Println("unsuccessful: ", body.Error)
			return errors.New(body.Error)
		}
		fmt.Println("success: ", body.Success)
		buf := bytes.NewReader(body.Diff)
		return b.Merge(ib, from, buf)
	}
}

func (b *ParallelBuilder) BuildLocal() (io.Reader, error) {
	if b.Node == nil {
		return nil, errors.New("No instruction specified")
	}
	ib := imagebuilder.NewBuilder(b.Options.Args)
	if err := b.executor.Prepare(ib, b.Node, b.From); err != nil {
		return nil, errors.Wrapf(err, "could not prepare build step")
	}
	defer b.executor.Delete()
	if err := b.executor.Execute(ib, b.Node); err != nil {
		return nil, errors.Wrapf(err, "could not execute step")
	}
	if err := b.executor.Commit(ib); err != nil {
		return nil, errors.Wrapf(err, "could not commit step")
	}
	image, err := getImage(b.From, b.executor.store)
	if err != nil {
		return nil, errors.Wrapf(err, "could not get image after committing")
	}
	diff, err := b.executor.store.Diff("", image.TopLayer, nil)
	if err != nil {
		return nil, errors.Wrapf(err, "could not get most recent diff")
	}
	return diff, nil
}

// Strip node's children and net values so that the node is treated as independent
func stripNode(node *parser.Node) {
	node.Children = nil
	node.Next = nil
}

func (b *ParallelBuilder) sendRequest(worker int) (*BuildRequest, error) {
	fmt.Println("in sendRequest()")
	url := "http://" + b.workers[worker] + ":8080"
	fmt.Println("url: ", url, ", worker: ", worker)
	// rotate through the list of workers atomically
	body, err := json.Marshal(b)
	if err != nil {
		return nil, errors.Wrapf(err, "could not get convert create json body")
	}
	fmt.Println("json marshalled")
	buf := bytes.NewBuffer(body)
	fmt.Println("created buffer")
	req, err := http.NewRequest("GET", url, buf)
	if err != nil {
		return nil, errors.Wrapf(err, "could not create request")
	}

	fmt.Println("sending request...")
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, errors.Wrapf(err, "Could not send request")
	}

	br := new(BuildRequest)
	err = json.Unmarshal(bodyToBytes(resp.Body), br)
	return br, err
}

func (b *ParallelBuilder) Merge(ib *imagebuilder.Builder, to string, diff io.Reader) error {
	fmt.Println("merging...")
	_, err := b.executor.store.ApplyDiff(to, diff)
	return err
}

func (b *ParallelBuilder) Commit(layer string) error {
	fmt.Println("committing...")
	_, err := b.executor.store.CreateImage("", []string{b.executor.output}, layer, "", &storage.ImageOptions{})
	return err
}

func deleteImage(image string, store storage.Store) error {
	img, err := getImage(image, store)
	if err != nil {
		return err
	}
	store.Delete(img.ID)
	return nil
}

func getImage(image string, store storage.Store) (*storage.Image, error) {
	var img *storage.Image
	ref, err := is.Transport.ParseStoreReference(store, image)
	if err == nil {
		img, err = is.Transport.GetStoreImage(store, ref)
	}
	if err != nil {
		img2, err2 := store.Image(image)
		if err2 != nil {
			if ref == nil {
				return nil, errors.Wrapf(err, "error parsing reference to image %q", image)
			}
			return nil, errors.Wrapf(err, "unable to locate image %q", image)
		}
		img = img2
	}
	return img, nil
}

func (b *ParallelBuilder) pullRootImage(ib *imagebuilder.Builder, node *parser.Node) (err error) {
	if err = b.executor.Prepare(ib, node, b.From); err != nil {
		return err
	}
	defer b.executor.Delete()
	if err = b.executor.Commit(ib); err != nil {
		return err
	}
	return nil
}

func (b *ParallelBuilder) createEmptyLayer(ib *imagebuilder.Builder, parent string) (string, error) {
	// get the storage.Image
	baseImageID, err := b.getImageID(parent)
	if err != nil {
		return "", nil
	}
	baseImage, err := b.executor.store.Image(baseImageID)
	if err != nil {
		return "", err
	}
	// get the top layer ID of the image
	topLayer := baseImage.TopLayer
	// Create an empty layer
	// Run Put() instead of Create() to create a digest
	layer, _, err := b.executor.store.PutLayer("", topLayer, []string{}, "", true, nil)
	if err != nil {
		return "", err
	}
	// Create an image from said layer
	names := make([]string, 1)
	names = append(names, b.executor.output)
	newImage, err := b.executor.store.CreateImage("", names, layer.ID, time.Now().String(), nil)
	return newImage.Names[0], err
}

func (b *ParallelBuilder) getImageID(name string) (string, error) {
	var img *storage.Image
	ref, err := is.Transport.ParseStoreReference(b.executor.store, name)
	if err == nil {
		img, err = is.Transport.GetStoreImage(b.executor.store, ref)
	}
	if err != nil {
		img2, err2 := b.executor.store.Image(name)
		if err2 != nil {
			if ref == nil {
				return "", errors.Wrapf(err, "error parsing reference to image %q", name)
			}
			return "", errors.Wrapf(err, "unable to locate image %q", name)
		}
		img = img2
	}
	return img.ID, nil

}

func bodyToBytes(body io.ReadCloser) []byte {
	defer body.Close()
	buf := new(bytes.Buffer)
	buf.ReadFrom(body)
	return buf.Bytes()
}
