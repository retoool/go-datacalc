package entity

type GetResponse struct {
	*Response
	Results []string `json:"results,omitempty"`
}

func NewGetResponse(code int) *GetResponse {
	gr := &GetResponse{
		Response: &Response{},
		Results:  nil,
	}
	gr.SetStatusCode(code)
	return gr
}

func (gr *GetResponse) GetResults() []string {
	return gr.Results
}
