package application

import (
	goContext "context"
	"fmt"
	"infrastructure/config"
	"infrastructure/context"
	"lib/logger"
	"lib/routine"
)

const (
	AppStateRunning = 1
	AppStateReload  = 2
	AppStateStop    = 3
)

var (
	appInstance = &App{}
)

//任务处理接口
type TaskHandler interface {
	Execute() error
}

type App struct {
	ctx      *context.BaseContext
	state    int16
	handlers map[string]TaskHandler
}

//初始化
func (app *App) initial() error {
	//init context
	rootCtx, cancelFunc := goContext.WithCancel(goContext.Background())
	app.ctx = &context.BaseContext{
		RootCtx:    rootCtx,
		CancelFunc: cancelFunc,
	}
	//init handler slice
	app.handlers = make(map[string]TaskHandler)
	//add
	if len(config.Conf.MysqlTask) > 0 {
		for _, conf := range config.Conf.MysqlTask {
			handler := NewMysqlHandler(app.Context())
			if err := handler.LoadConfig(conf); err != nil {
				logger.Error("[application.initial] mysql handler load config err:%s", err.Error())
				return err
			}
			app.AddHandler(handler.name, handler)
		}
	}
	return nil
}

//设置状态
func (app *App) SetState(state int16) {
	app.state = state
}

//获取根上下文
func (app *App) Context() *context.BaseContext {
	return app.ctx
}

//添加处理者
func (app *App) AddHandler(name string, handler TaskHandler) {
	app.handlers[name] = handler
}

//运行
func (app *App) Run() error {
	//check state
	if app.state == AppStateRunning {
		return fmt.Errorf("app is runnig now")
	}
	//init
	if err := app.initial(); err != nil {
		logger.Error("[application.Run] app.initial err:%s", err.Error())
		return err
	}
	//run
	for _, h := range app.handlers {
		if err := h.Execute(); err != nil {
			logger.Error("[App.Run] handle execute err:%s", err.Error())
			return err
		}
	}
	//change state
	app.SetState(AppStateRunning)
	logger.Info("[App.Run] app run success.")
	return nil
}

//重启
func (app *App) Reload() error {
	//check state
	if app.state != AppStateRunning {
		return fmt.Errorf("reload error. app not runnig(%d)", app.state)
	}
	//set state
	app.SetState(AppStateReload)
	logger.Info("[App.Reload] app begin reload...")
	//context cancel
	app.ctx.CancelFunc()
	//等待业务routine退出
	routine.Wait()
	//重启
	if err := app.Run(); err != nil {
		logger.Error("[App.Reload] app.run err:%s", err.Error())
		return err
	}
	logger.Info("[App.Reload] app reload success.")
	return nil
}

//停止
func (app *App) Stop() error {
	//check state
	if app.state != AppStateRunning {
		return fmt.Errorf("stop error. app not runnig(%d), ", app.state)
	}
	//set state
	app.SetState(AppStateStop)
	logger.Info("[App.Stop] app begin stop...")
	//context cancel
	app.ctx.CancelFunc()
	app.handlers = make(map[string]TaskHandler)
	//等待业务routine退出
	routine.Wait()
	logger.Info("[App.Stop] app stop success.")
	return nil
}

func Run() error {
	//run
	if err := appInstance.Run(); err != nil {
		logger.Error("[application.Run] appInstance.Run err:%s", err.Error())
		return err
	}
	return nil
}

func Reload() error {
	if err := appInstance.Reload(); err != nil {
		logger.Error("[application.Reload] appInstance.Reload err:%s", err.Error())
		return err
	}
	return nil
}

func Stop() error {
	if err := appInstance.Stop(); err != nil {
		logger.Error("[application.close] appInstance.close err:%s", err.Error())
		return err
	}
	return nil
}
