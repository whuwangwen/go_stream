package go_stream

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestStream(t *testing.T) {
	timeForm := time.Now()
	s := make([]interface{}, 0)
	for i := 0; i < 1000; i++ {
		s = append(s, i)
	}

	map1Func := func(ctx context.Context, i interface{}) interface{} {
		intI := i.(int)
		intI++
		//time.Sleep(100 * time.Microsecond)
		return intI

	}

	map2Func := func(ctx context.Context, i interface{}) interface{} {
		intI := i.(int)
		intI = intI * 2
		//time.Sleep(200 * time.Microsecond)
		return intI
	}

	map3Func := func(ctx context.Context, i interface{}) interface{} {
		intI := i.(int)
		intI = intI + 5
		//time.Sleep(100 * time.Microsecond)
		return intI
	}
	res := AsStream(context.Background(), s, 5, nil, nil).MapTo(map1Func, 5).MapTo(map2Func, 10).MapTo(map3Func, 5).CollectAsList()

	fmt.Println(res)
	timeEnd := time.Now()
	fmt.Println(timeEnd.UnixMilli() - timeForm.UnixMilli())

}

func TestSG(t *testing.T) {
	timeForm := time.Now()
	s := make([]int, 0)
	for i := 0; i < 10000000; i++ {
		s = append(s, i)
	}
	for _, i := range s {
		i++
		i = i * 2
		i += 5
		//fmt.Println(i)
	}
	timeEnd := time.Now()
	fmt.Println(timeEnd.UnixMilli() - timeForm.UnixMilli())
}
