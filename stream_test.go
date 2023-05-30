package go_stream

import (
	"context"
	"fmt"
	"testing"
)

func TestStream(t *testing.T) {
	s := make([]interface{}, 0)
	for i := 0; i < 10000000; i++ {
		s = append(s, i)
	}

	map1Func := func(ctx context.Context, i interface{}) interface{} {
		intI := i.(int)
		intI++
		return intI
	}

	map2Func := func(ctx context.Context, i interface{}) interface{} {
		intI := i.(int)
		intI = intI * 2
		return intI
	}

	map3Func := func(ctx context.Context, i interface{}) interface{} {
		intI := i.(int)
		intI = intI + 5
		return intI
	}
	res := AsStream(context.Background(), s, 5).MapTo(map1Func, 2).MapTo(map2Func, 3).MapTo(map3Func, 4).CollectAsList()

	fmt.Println(res)

}
