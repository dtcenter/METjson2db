package main

type IStatToCbBuilder interface {
	processFields() bool
}

func getBuilder(lineType string, coldef ColDefArray, fields []string) IStatToCbBuilder {
	if lineType == "SL1L2" {
		return newStatToCbBuilder_SL1L2(coldef, fields)
	}

	/*
		if lineType == "SAL1L2" {
			return StatToCbBuilder_SAL1L2()
		}
	*/
	return nil
}
