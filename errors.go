package riskybiscuits

import (
	"fmt"

	"cuelang.org/go/cue/errors"
	"cuelang.org/go/cue/token"
)

type PortablePosition struct {
	Filepath string `json:"filepath"`
	Offset   int    `json:"offset"`
}

func ErrorToPortable(err errors.Error) *PortableError {
	poss := err.InputPositions()
	possJson := make([]PortablePosition, len(poss))
	for i, pos := range poss {
		possJson[i] = PosToPortable(pos)
	}
	f, args := err.Msg()
	return &PortableError{
		PositionJSON:       PosToPortable(err.Position()),
		InputPositionsJSON: possJson,
		ErrorJSON:          err.Error(),
		PathJSON:           err.Path(),
		MsgJSON:            fmt.Sprintf(f, args...),
	}
}

func PosToPortable(p token.Pos) PortablePosition {
	if p == token.NoPos {
		return PortablePosition{}
	}
	return PortablePosition{
		Filepath: p.Filename(),
		Offset:   p.Offset(),
	}
}

type PortableError struct {
	PositionJSON       PortablePosition   `json:"position"`
	InputPositionsJSON []PortablePosition `json:"input_positions"`
	ErrorJSON          string             `json:"error"`
	PathJSON           []string           `json:"paths"`
	MsgJSON            string             `json:"msg"`
}

func (p *PortableError) Error() string {
	return p.ErrorJSON
}

func (p *PortableError) Path() []string {
	return p.PathJSON
}

func (p *PortableError) Msg() (format string, args []interface{}) {
	return "%s", []interface{}{p.MsgJSON}
}
