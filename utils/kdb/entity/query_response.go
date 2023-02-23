package entity

type GroupResult struct {
	Name string `json:"name,omitempty"`
}

type Results struct {
	Name       string              `json:"name,omitempty"`
	DataPoints []DataPoint         `json:"values,omitempty"`
	Tags       map[string][]string `json:"tags,omitempty"`
	Group      []GroupResult       `json:"group_by,omitempty"`
}

type Queries struct {
	SampleSize int64     `json:"sample_size,omitempty"`
	ResultsArr []Results `json:"results,omitempty"`
}

type QueryResponse struct {
	*Response
	QueriesArr []Queries `json:"queries",omitempty`
}

func NewQueryResponse(code int) *QueryResponse {
	qr := &QueryResponse{
		Response: &Response{},
	}

	qr.SetStatusCode(code)
	return qr
}
