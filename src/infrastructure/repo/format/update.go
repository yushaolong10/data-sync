package format

type UpdateFormat struct {
	Type        string                 `json:"type"`
	Database    string                 `json:"database"`
	Table       string                 `json:"table"`
	PrimaryVals []interface{}          `json:"primary_key"`
	PrimaryCols []string               `json:"primary_key_columns"`
	Data        map[string]interface{} `json:"data"`
	Old         map[string]interface{} `json:"old"`
}

//{
//    "database": "test",
//    "table": "maxwell",
//    "type": "update",
//    "ts": 1449786341,
//    "xid": 940786,
//    "commit": true,
//    "data": {"id":1, "daemon": "Firebus!  Firebus!"},
//    "old":  {"daemon": "Stanislaw Lem"}
//  }
