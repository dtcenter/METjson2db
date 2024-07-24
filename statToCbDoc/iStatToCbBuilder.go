package main

type IStatToCbBuilder interface {
	setWindowType()
	setDoorType()
	setNumFloor()
	getHouse()
}

func getBuilder(lineType string) IStatToCbBuilder {
	if lineType == "SL1L2" {
		return newStatToCbBuilder_SL1L2()
	}

	/*
		if lineType == "SAL1L2" {
			return StatToCbBuilder_SAL1L2()
		}
	*/
	return nil
}
