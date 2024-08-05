package main

type IStatToCbBuilder interface {
	processFields(fields) bool
	setFileName()
}

func getBuilder(lineType string, fields: [] string) IStatToCbBuilder {
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
