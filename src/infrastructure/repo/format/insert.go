package format

type InsertFormat struct {
	Type        string                 `json:"type"`
	Database    string                 `json:"database"`
	Table       string                 `json:"table"`
	PrimaryVals []interface{}          `json:"primary_key"`
	PrimaryCols []string               `json:"primary_key_columns"`
	Data        map[string]interface{} `json:"data"`
}

//{
//   "database":"test",
//   "table":"e",
//   "type":"insert",
//   "ts":1477053217,
//   "xid":23396,
//   "commit":true,
//   "position":"master.000006:800911",
//   "server_id":23042,
//   "thread_id":108,
//   "primary_key": [1, "2016-10-21 05:33:37.523000"],
//   "primary_key_columns": ["id", "c"],
//   "data":{
//      "id":1,
//      "m":4.2341,
//      "c":"2016-10-21 05:33:37.523000",
//      "comment":"I am a creature of light."
//   }
//}
