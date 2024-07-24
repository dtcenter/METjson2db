package main

type StatToCbBuilder_SL1L2 struct {
	windowType string
	doorType   string
	floor      int
}

// getHouse implements IStatToCbBuilder.
func (b *StatToCbBuilder_SL1L2) getHouse() {
	panic("unimplemented")
}

func newStatToCbBuilder_SL1L2() IStatToCbBuilder {
	return &StatToCbBuilder_SL1L2{}
}

func (b *StatToCbBuilder_SL1L2) setWindowType() {
	b.windowType = "Wooden Window"
}

func (b *StatToCbBuilder_SL1L2) setDoorType() {
	b.doorType = "Wooden Door"
}

func (b *StatToCbBuilder_SL1L2) setNumFloor() {
	b.floor = 2
}
