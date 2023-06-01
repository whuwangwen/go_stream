package go_stream

import (
	"context"
	"log"
	"sync"
)

const defaultStageGCount = 1

type Stream struct {
	d                []interface{}
	i                *inter
	stageOffset      int
	chanSize         int
	stage            *StreamStage
	ctx              context.Context
	cc               *chanChain
	beforeProcessors []BeforeProcessor //可以实现细粒度的日志追踪
	afterProcessors  []AfterProcessor
}

type StreamStage struct {
	i *inter
	o *outer
}

type chanChain struct {
	c    chan interface{}
	next *chanChain
	w    *sync.WaitGroup
}
type inter struct {
	inChan chan interface{}
}

type outer struct {
	outChan chan interface{}
}

func AsStream(ctx context.Context, slice []interface{}, chanSize int, beforeProcessors []BeforeProcessor, afterProcessors []AfterProcessor) *Stream {
	inChan := make(chan interface{}, chanSize)
	interRoot := &inter{
		inChan: inChan,
	}
	stream := new(Stream)
	stream.i = interRoot
	stream.d = slice
	stream.chanSize = chanSize

	stageRoot := new(StreamStage)
	stageRoot.i = &inter{inChan: inChan}

	stream.stage = stageRoot
	stream.ctx = ctx
	stream.cc = &chanChain{
		c: inChan,
	}
	stream.beforeProcessors = beforeProcessors
	stream.afterProcessors = afterProcessors
	//stream.refreshStreamStage()
	return stream
}

func (s *Stream) MapTo(function MapFunction, gStageCount int) *Stream {
	s.doStream(function, gStageCount)
	return s
}

func (s *Stream) doStream(function MapFunction, gStageCount int) {
	o := make(chan interface{}, s.chanSize)

	curStage := s.stage
	curStage.o = &outer{
		outChan: o,
	}

	curIn := curStage.i.inChan
	var w sync.WaitGroup //用于判断此次协程全部执行完毕
	if gStageCount == 0 {
		gStageCount = defaultStageGCount
	}
	w.Add(gStageCount)
	s.addChanChain(o, &w)
	//启动指定的协程数消费
	gCount := 0
	for {
		if gCount >= gStageCount {
			break
		}
		go func(step int) {
			defer func() {
				w.Done()
			}()
			//多协程消费chan
			for m := range curIn {
				o <- s.secureProcess(function, m)
			}
		}(s.stageOffset)
		gCount++
	}

	//go func() {
	//	//当前stage 全部退出时，关闭out chan
	//	w.Wait()
	//	close(o)
	//}()

	s.refreshStreamStage()
}

func (s *Stream) addChanChain(c chan interface{}, w *sync.WaitGroup) {
	ccRoot := s.cc
	ccCur := ccRoot
	var ccPrev *chanChain
	for {
		if ccCur != nil {
			ccPrev = ccCur
			ccCur = ccCur.next
		} else {
			break
		}
	}
	ccPrev.next = &chanChain{c: c, w: w}
}

func (s *Stream) closeChanChain() {
	ccRoot := s.cc
	ccCur := ccRoot
	for {
		if ccCur != nil {
			if ccCur.w != nil {
				go func(cc *chanChain) {
					cNow := cc
					for {
						if cNow != nil {
							cNow.w.Wait()
							close(cNow.c)
						} else {
							break
						}
						cNow = cNow.next
					}
				}(ccCur)
				break
			} else {
				close(ccCur.c)
			}
			ccCur = ccCur.next
		} else {
			break
		}
	}
}

// 避免MapFunction panic导致协程数量减少，最终输出的数据可能由于recover 比初始的数量少
func (s *Stream) secureProcess(function MapFunction, param interface{}) interface{} {
	defer func() {
		if err := recover(); err != nil {
			log.Printf("process err happened:%s", err)
		}
	}()
	s.beforeProcess(s.ctx, param)
	res := function(s.ctx, param)
	s.afterProcess(s.ctx, param, res)
	return res
}

func (s *Stream) beforeProcess(ctx context.Context, param interface{}) {
	for _, beforeProcessor := range s.beforeProcessors {
		beforeProcessor(ctx, param)
	}
}

func (s *Stream) afterProcess(ctx context.Context, param interface{}, res interface{}) {
	for _, afterProcessor := range s.afterProcessors {
		afterProcessor(ctx, param, res)
	}
}

func (s *Stream) CollectAsList() []interface{} {
	outerChan := s.stage.i.inChan
	var w sync.WaitGroup
	w.Add(1)
	res := make([]interface{}, 0)
	go func() {
		defer w.Done()
		for om := range outerChan {
			res = append(res, om)
		}
	}()

	for _, data := range s.d {
		s.i.inChan <- data
	}
	//关闭初始的chan 驱动流
	s.closeChanChain()

	//阻塞等待收集完成
	w.Wait()
	return res
}

func (s *Stream) refreshStreamStage() {
	s.stageOffset++
	curStage := s.stage
	curStage.i.inChan = curStage.o.outChan
}

type MapFunction func(ctx context.Context, i interface{}) interface{}

type BeforeProcessor func(ctx context.Context, i interface{})

type AfterProcessor func(ctx context.Context, i interface{}, res interface{})
