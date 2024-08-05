package main

type StatToCbBuilder_SL1L2 struct {
	fileName string
	fields   []string
}

/*
// getHouse implements IStatToCbBuilder.
func (b *StatToCbBuilder_SL1L2) getHouse() {
	panic("unimplemented")
}
*/

func newStatToCbBuilder_SL1L2() IStatToCbBuilder {
	return &StatToCbBuilder_SL1L2{}
}

func (b *StatToCbBuilder_SL1L2) setFileName() {
	b.fileName = ""
}

func (b *StatToCbBuilder_SL1L2) processFields() bool {
	return true
}
