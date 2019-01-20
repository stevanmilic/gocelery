package gocelery

import (
	"context"
	"log"
	"runtime"
	"sync"

	"github.com/go-redis/redis"
	"github.com/gocelery/gocelery/backends"
	"github.com/gocelery/gocelery/brokers"
	"github.com/gocelery/gocelery/parsers"
	"github.com/gocelery/gocelery/protocol"
)

// Worker represents distributed worker
type Worker struct {
	options         *WorkerOptions
	ctx             context.Context
	ctxCancel       context.CancelFunc
	registeredTasks sync.Map
	waitGroup       sync.WaitGroup
}

// NewWorker creates new celery workers
// number of workers is set to number of cpu cores if numWorkers smaller than zero
func NewWorker(ctx context.Context, options *WorkerOptions) (*Worker, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	options.sanitize()
	wCtx, cancel := context.WithCancel(ctx)
	return &Worker{
		options:   options,
		ctx:       wCtx,
		ctxCancel: cancel,
	}, nil
}

// WorkerOptions define options for worker
type WorkerOptions struct {
	Broker         Broker
	Backend        Backend
	Parser         Parser
	NumWorkers     int
	MsgProtocolVer int
}

func (o *WorkerOptions) sanitize() {
	if o.Broker == nil || o.Backend == nil {
		client := redis.NewClient(&redis.Options{})
		if o.Broker == nil {
			o.Broker = &brokers.RedisBroker{Client: client}
		}
		if o.Backend == nil {
			o.Backend = &backends.RedisBackend{Client: client}
		}
	}
	if o.Parser == nil {
		o.Parser = &parsers.JSONParser{}
	}
	if o.NumWorkers <= 0 {
		o.NumWorkers = runtime.NumCPU()
	}
	if o.MsgProtocolVer != 1 {
		o.MsgProtocolVer = 2
	}
}

// Start starts all celery workers
func (w *Worker) Start() {
	w.waitGroup.Add(w.options.NumWorkers)
	for i := 0; i < w.options.NumWorkers; i++ {
		go func(workerID int) {
			log.Printf("running worker %d", workerID)

			defer w.waitGroup.Done()

			for {
				select {
				case <-w.ctx.Done():
					return
				default:

					// get message from broker
					rawMsg, err := w.options.Broker.Get()
					if err != nil || rawMsg == nil {
						log.Printf("broker error: %v", err)
						continue
					}

					// parse task message
					// TODO: support multiple formats
					tm, err := w.options.Parser.Decode(rawMsg)
					if err != nil {
						log.Printf("parser error: %v", err)
						continue
					}

					// validate task message
					if err := tm.Validate(
						w.options.Parser.ContentType(),
						w.options.MsgProtocolVer); err != nil {
						log.Printf("task message format error: %v", err)
						continue
					}

					log.Printf("task message: %v", tm)

					// TODO: parse message body (base64 encoding)

					// TODO: run task

					// TODO: push results to backend

				}
			}
		}(i)
	}
}

// Stop gracefully stops all celery workers
// and waits for all workers to stop
func (w *Worker) Stop() {
	w.ctxCancel()
	w.waitGroup.Wait()
}

// Register new task
func (w *Worker) Register(name string, task interface{}) {
	w.registeredTasks.Store(name, task)
}

func (w *Worker) runTask(msg *protocol.TaskMessage) {
	// get task
	//w.registeredTasks.Load(msg.)
}
