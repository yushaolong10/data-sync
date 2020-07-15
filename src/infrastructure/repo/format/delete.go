package format

type DeleteFormat struct {
	Type        string                 `json:"type"`
	Database    string                 `json:"database"`
	Table       string                 `json:"table"`
	PrimaryVals []interface{}          `json:"primary_key"`
	PrimaryCols []string               `json:"primary_key_columns"`
	Data        map[string]interface{} `json:"data"`
}

//mysql> delete from test.e where id = 1;
//{
//   "database":"test",
//   "table":"e",
//   "type":"delete",
//   ...
//   "data":{
//      "id":1,
//      "m":5.444,
//      "c":"2016-10-21 05:33:54.631000",
//      "comment":"I am a creature of light."
//   }
//}
