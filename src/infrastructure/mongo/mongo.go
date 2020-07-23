package mongo

import (
	"errors"
	"infrastructure/config"
	"infrastructure/monitor"
	"lib/logger"
	"lib/mongo"
	"lib/util"
)

var (
	mongoPtr *mongo.MongoClient
)

func Init() error {
	conf := &config.Conf.MongoConf
	mongoPtr = mongo.NewMongoClient(conf.Enable, conf.DNS, conf.Timeout, conf.Mode, conf.MaxPool)
	if mongoPtr.Enable() {
		if err := mongoPtr.Start(); err != nil {
			return err
		}
	} else {
		logger.Info("[mongo] client disable")
	}
	return nil
}

func Close() {
	if mongoPtr.Enable() {
		mongoPtr.Close()
	}
}

func FindOne(db string, col string, query interface{}) (ret map[string]interface{}, err error) {
	if mongoPtr.Enable() {
		beg := util.GetCurrTimeMicors()
		ret, err = mongoPtr.FindOne(db, col, query)
		interval := util.GetCurrTimeMicors() - beg
		monitor.UpdateDependence("mongo", "FindOne", interval, err)
	} else {
		return nil, errors.New("mongo disable")
	}
	return
}

func One(db string, col string, query interface{}, res interface{}) (ret bool, err error) {
	if mongoPtr.Enable() {
		beg := util.GetCurrTimeMicors()
		ret, err = mongoPtr.One(db, col, query, res)
		interval := util.GetCurrTimeMicors() - beg
		monitor.UpdateDependence("mongo", "One", interval, err)
	} else {
		return false, errors.New("mongo disable")
	}
	return
}

func Find(db string, col string, query interface{}, sort interface{}) (ret []map[string]interface{}, err error) {
	if mongoPtr.Enable() {
		beg := util.GetCurrTimeMicors()
		ret, err = mongoPtr.Find(db, col, query, sort)
		interval := util.GetCurrTimeMicors() - beg
		monitor.UpdateDependence("mongo", "Find", interval, err)
	} else {
		return nil, errors.New("mongo disable")
	}
	return
}

func FindWithRes(db string, col string, query interface{}, sort []string, limit [2]int, res interface{}) (err error) {
	if mongoPtr.Enable() {
		beg := util.GetCurrTimeMicors()
		err = mongoPtr.FindWithRes(db, col, query, sort, limit, res)
		interval := util.GetCurrTimeMicors() - beg
		monitor.UpdateDependence("mongo", "FindWithRes", interval, err)
	} else {
		return errors.New("mongo disable")
	}
	return
}

func Insert(db, col string, doc interface{}) (err error) {
	if mongoPtr.Enable() {
		beg := util.GetCurrTimeMicors()
		err = mongoPtr.Insert(db, col, doc)
		interval := util.GetCurrTimeMicors() - beg
		monitor.UpdateDependence("mongo", "Insert", interval, err)
	} else {
		return errors.New("mongo disable")
	}
	return
}

func Update(db, col string, selector interface{}, doc interface{}) (err error) {
	if mongoPtr.Enable() {
		beg := util.GetCurrTimeMicors()
		err = mongoPtr.Update(db, col, selector, doc)
		interval := util.GetCurrTimeMicors() - beg
		monitor.UpdateDependence("mongo", "Update", interval, err)
	} else {
		return errors.New("mongo disable")
	}
	return
}

func Upsert(db, col string, selector interface{}, doc interface{}) (err error) {
	if mongoPtr.Enable() {
		beg := util.GetCurrTimeMicors()
		err = mongoPtr.Upsert(db, col, selector, doc)
		interval := util.GetCurrTimeMicors() - beg
		monitor.UpdateDependence("mongo", "Upsert", interval, err)
	} else {
		return errors.New("mongo disable")
	}
	return
}

func Remove(db, col string, selector interface{}) (err error) {
	if mongoPtr.Enable() {
		beg := util.GetCurrTimeMicors()
		err = mongoPtr.Remove(db, col, selector)
		interval := util.GetCurrTimeMicors() - beg
		monitor.UpdateDependence("mongo", "Remove", interval, err)
	} else {
		return errors.New("mongo disable")
	}
	return
}

func Count(db, col string, selector interface{}) (ret int, err error) {
	if mongoPtr.Enable() {
		beg := util.GetCurrTimeMicors()
		ret, err = mongoPtr.Count(db, col, selector)
		interval := util.GetCurrTimeMicors() - beg
		monitor.UpdateDependence("mongo", "Count", interval, err)
	} else {
		return 0, errors.New("mongo disable")
	}
	return
}

func Aggregate(db, col string, pipeline interface{}, res interface{}) (err error) {
	if mongoPtr.Enable() {
		beg := util.GetCurrTimeMicors()
		err = mongoPtr.Aggregate(db, col, pipeline, res)
		interval := util.GetCurrTimeMicors() - beg
		monitor.UpdateDependence("mongo", "Aggregate", interval, err)
	} else {
		return errors.New("mongo disable")
	}
	return
}
