package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// EventType defines the type of event emitted by a Rangefeed.
type EventType int

const (
	EventMutation EventType = iota
	EventResolvedTS
)

// Event represents a mutation or a resolved timestamp.
type Event struct {
	Type      EventType
	RangeID   int
	Key       string
	Val       string
	Timestamp uint64
}

// Cluster simulates the CockroachDB cluster state.
type Cluster struct {
	mu           sync.Mutex
	leaseholders map[int]int // rangeID -> nodeID
	processors   map[int]*RangefeedProcessor
	currentTime  uint64
}

func NewCluster() *Cluster {
	return &Cluster{
		leaseholders: make(map[int]int),
		processors:   make(map[int]*RangefeedProcessor),
		currentTime:  10,
	}
}

func (c *Cluster) GetLeaseholder(rangeID int) (int, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	nodeID, ok := c.leaseholders[rangeID]
	return nodeID, ok
}

func (c *Cluster) TransferLease(rangeID int, newLeaseholder int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	// Stop the old processor if any
	if oldNode, ok := c.leaseholders[rangeID]; ok {
		if p, ok := c.processors[oldNode*1000+rangeID]; ok {
			p.Stop()
		}
	}
	
	c.leaseholders[rangeID] = newLeaseholder
	fmt.Printf("[Cluster] Transferred lease for range %d to node %d\n", rangeID, newLeaseholder)
}

func (c *Cluster) RegisterProcessor(rangeID int, nodeID int, p *RangefeedProcessor) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.processors[nodeID*1000+rangeID] = p
}

func (c *Cluster) GetProcessor(rangeID int, nodeID int) *RangefeedProcessor {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.processors[nodeID*1000+rangeID]
}

func (c *Cluster) AdvanceTime() uint64 {
	return atomic.AddUint64(&c.currentTime, 1)
}

func (c *Cluster) GetTime() uint64 {
	return atomic.LoadUint64(&c.currentTime)
}

// RangefeedProcessor simulates a range leaseholder emitting updates.
type RangefeedProcessor struct {
	rangeID    int
	nodeID     int
	cluster    *Cluster
	mu         sync.Mutex
	active     bool
	intents    map[string]uint64 // key -> timestamp
	stopChan   chan struct{}
}

func NewRangefeedProcessor(rangeID int, nodeID int, cluster *Cluster) *RangefeedProcessor {
	p := &RangefeedProcessor{
		rangeID:  rangeID,
		nodeID:   nodeID,
		cluster:  cluster,
		intents:  make(map[string]uint64),
		stopChan: make(chan struct{}),
	}
	cluster.RegisterProcessor(rangeID, nodeID, p)
	return p
}

func (p *RangefeedProcessor) WriteIntent(key string, ts uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.intents[key] = ts
}

func (p *RangefeedProcessor) CommitIntent(key string, ts uint64, eventChan chan<- Event) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.intents[key]; ok {
		delete(p.intents, key)
		if eventChan != nil {
			eventChan <- Event{
				Type:      EventMutation,
				RangeID:   p.rangeID,
				Key:       key,
				Val:       "val",
				Timestamp: ts,
			}
		}
	}
}

func (p *RangefeedProcessor) Stop() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.active {
		p.active = false
		close(p.stopChan)
	}
}

func (p *RangefeedProcessor) StartRangefeed(
	ctx context.Context,
	startTS uint64,
	outChan chan<- Event,
) error {
	p.mu.Lock()
	if p.active {
		p.mu.Unlock()
		return errors.New("rangefeed already active")
	}
	p.active = true
	p.stopChan = make(chan struct{})
	p.mu.Unlock()

	// Emit initial scan of intents >= startTS
	p.mu.Lock()
	for key, ts := range p.intents {
		if ts >= startTS {
			outChan <- Event{
				Type:      EventMutation,
				RangeID:   p.rangeID,
				Key:       key,
				Val:       "intent",
				Timestamp: ts,
			}
		}
	}
	p.mu.Unlock()

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-p.stopChan:
			return errors.New("rangefeed stopped (lease lost)")
		case <-ticker.C:
			p.mu.Lock()
			if !p.active {
				p.mu.Unlock()
				return errors.New("rangefeed stopped (lease lost)")
			}

			// Compute resolved timestamp
			currTime := p.cluster.GetTime()
			resolvedTS := currTime

			for _, intentTS := range p.intents {
				if intentTS < resolvedTS {
					resolvedTS = intentTS
				}
			}

			// CRITICAL FIX: The resolved timestamp emitted must never be lower than startTS.
			// This prevents the resolved timestamp from regressing or stalling the aggregator's frontier.
			if resolvedTS < startTS {
				resolvedTS = startTS
			}

			outChan <- Event{
				Type:      EventResolvedTS,
				RangeID:   p.rangeID,
				Timestamp: resolvedTS,
			}
			p.mu.Unlock()
		}
	}
}

// ChangefeedAggregator tracks the resolved timestamp frontier across ranges.
type ChangefeedAggregator struct {
	mu                sync.Mutex
	rangeResolvedTS   map[int]uint64
	globalFrontier    uint64
	mutationsReceived []Event
}

func NewChangefeedAggregator(rangeIDs []int) *ChangefeedAggregator {
	rts := make(map[int]uint64)
	for _, rid := range rangeIDs {
		rts[rid] = 0
	}
	return &ChangefeedAggregator{
		rangeResolvedTS: rts,
		globalFrontier:  0,
	}
}

func (a *ChangefeedAggregator) UpdateResolvedTS(rangeID int, ts uint64) {
	a.mu.Lock()
	defer a.mu.Unlock()

	// Monotonicity check per range
	if ts > a.rangeResolvedTS[rangeID] {
		a.rangeResolvedTS[rangeID] = ts
		a.recomputeFrontier()
	}
}

func (a *ChangefeedAggregator) recomputeFrontier() {
	var minTS uint64 = ^uint64(0)
	for _, ts := range a.rangeResolvedTS {
		if ts < minTS {
			minTS = ts
		}
	}
	if minTS > a.globalFrontier {
		a.globalFrontier = minTS
		fmt.Printf("[Aggregator] Global Frontier advanced to %d\n", minTS)
	}
}

func (a *ChangefeedAggregator) GetFrontier() uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.globalFrontier
}

func (a *ChangefeedAggregator) RecordMutation(ev Event) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.mutationsReceived = append(a.mutationsReceived, ev)
}

func (a *ChangefeedAggregator) GetMutations() []Event {
	a.mu.Lock()
	defer a.mu.Unlock()
	copied := make([]Event, len(a.mutationsReceived))
	copy(copied, a.mutationsReceived)
	return copied
}

// RangefeedClient manages connection to the active leaseholder.
type RangefeedClient struct {
	rangeID    int
	cluster    *Cluster
	aggregator *ChangefeedAggregator
	mu         sync.Mutex
	lastSeenTS uint64
}

func NewRangefeedClient(rangeID int, cluster *Cluster, aggregator *ChangefeedAggregator) *RangefeedClient {
	return &RangefeedClient{
		rangeID:    rangeID,
		cluster:    cluster,
		aggregator: aggregator,
	}
}

func (c *RangefeedClient) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Find the current leaseholder
		nodeID, ok := c.cluster.GetLeaseholder(c.rangeID)
		if !ok {
			time.Sleep(10 * time.Millisecond)
			continue
		}

		processor := c.cluster.GetProcessor(c.rangeID, nodeID)
		if processor == nil {
			time.Sleep(10 * time.Millisecond)
			continue
		}

		// CRITICAL FIX: Determine the start timestamp for the new rangefeed connection.
		// It must be the maximum of the last seen resolved timestamp for this range
		// and the aggregator's current global frontier.
		// This ensures that we do not request a start timestamp that is older than the
		// aggregator's current frontier, which would cause the new leaseholder to initialize
		// its resolved timestamp at a value lower than the frontier and potentially stall.
		c.mu.Lock()
		startTS := c.lastSeenTS
		globalFrontier := c.aggregator.GetFrontier()
		if globalFrontier > startTS {
			startTS = globalFrontier
		}
		c.mu.Unlock()

		eventChan := make(chan Event, 100)
		
		// Run the rangefeed
		runCtx, cancel := context.WithCancel(ctx)
		
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-runCtx.Done():
					return
				case ev, ok := <-eventChan:
					if !ok {
						return
					}
					if ev.Type == EventMutation {
						c.aggregator.RecordMutation(ev)
					} else if ev.Type == EventResolvedTS {
						c.mu.Lock()
						if ev.Timestamp > c.lastSeenTS {
							c.lastSeenTS = ev.Timestamp
						}
						c.mu.Unlock()
						c.aggregator.UpdateResolvedTS(c.rangeID, ev.Timestamp)
					}
				}
			}
		}()

		err := processor.StartRangefeed(runCtx, startTS, eventChan)
		cancel()
		close(eventChan)
		wg.Wait()

		if err != nil {
			fmt.Printf("[Client] Rangefeed for range %d disconnected: %v. Reconnecting...\n", c.rangeID, err)
		}
		
		// Small backoff before reconnecting
		time.Sleep(10 * time.Millisecond)
		}
}

func main() {
	fmt.Println("Starting Changefeed Resolved Timestamp Frontier Stall Regression Test...")

	cluster := NewCluster()
	rangeIDs := []int{1}
	aggregator := NewChangefeedAggregator(rangeIDs)

	// Setup Node 1 and Node 2
	node1 := 1
	node2 := 2
	p1 := NewRangefeedProcessor(1, node1, cluster)
	p2 := NewRangefeedProcessor(1, node2, cluster)

	// Set initial leaseholder to Node 1
	cluster.TransferLease(1, node1)

	// Start Rangefeed Client
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := NewRangefeedClient(1, cluster, aggregator)
	go client.Run(ctx)

	// Start background writer to simulate sustained write workload
	stopWriter := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopWriter:
				return
			case <-ticker.C:
				ts := cluster.AdvanceTime()
				// Write and commit immediately to advance resolved timestamp
				p1.WriteIntent("key", ts)
				p1.CommitIntent("key", ts, nil)
				p2.WriteIntent("key", ts)
				p2.CommitIntent("key", ts, nil)
			}
		}
	}()

	// Wait for frontier to advance past 20
	fmt.Println("Waiting for initial frontier advancement...")
	for {
		if aggregator.GetFrontier() > 20 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	initialFrontier := aggregator.GetFrontier()
	fmt.Printf("Initial frontier reached: %d\n", initialFrontier)

	// Simulate an uncommitted intent on Node 2 at a timestamp lower than the current frontier
	// to trigger the potential stall condition during lease transfer.
	staleIntentTS := initialFrontier - 5
	p2.WriteIntent("stale_key", staleIntentTS)

	// Trigger lease transfer to Node 2
	cluster.TransferLease(1, node2)

	// Wait to see if the frontier continues to advance past the lease transfer event
	fmt.Println("Waiting to verify frontier continues to advance after lease transfer...")
	success := false
	for i := 0; i < 50; i++ {
		currentFrontier := aggregator.GetFrontier()
		if currentFrontier > initialFrontier+10 {
			fmt.Printf("Success! Frontier advanced to %d\n", currentFrontier)
			success = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	close(stopWriter)
	wg.Wait()

	if !success {
		panic("TEST FAILED: Changefeed resolved timestamp frontier stalled after lease transfer!")
	}
	fmt.Println("TEST PASSED: Changefeed resolved timestamp frontier advanced successfully during and after lease transfer.")
}
