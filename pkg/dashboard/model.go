package dashboard

import "time"

type Snapshot struct {
	GeneratedAt time.Time `json:"generatedAt"`
	Nodes       []Node    `json:"nodes"`
	Edges       []Edge    `json:"edges"`
}

type Node struct {
	ID       string            `json:"id"`
	ParentID string            `json:"parentId,omitempty"`
	Kind     string            `json:"kind"`
	Shape    string            `json:"shape,omitempty"`
	Name     string            `json:"name"`
	Group    string            `json:"group"`
	Status   string            `json:"status"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

type Edge struct{ From, To, Kind string }
