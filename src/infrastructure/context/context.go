package context

import "context"

type BaseContext struct {
	RootCtx    context.Context
	CancelFunc context.CancelFunc
}

func (b *BaseContext) IsCanceled() bool {
	//canceled , err is not nil
	return b.RootCtx.Err() != nil
}

type BizContext struct {
	*BaseContext
	RequestId string
}

func NewBizContext(ctx *BaseContext, requestId string) *BizContext {
	return &BizContext{
		RequestId:   requestId,
		BaseContext: ctx,
	}
}
