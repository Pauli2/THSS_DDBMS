package models

type Dataset struct {
	Schema TableSchema
	Rows []Row
}

func (d *Dataset) AppendNoRec(newrow Row) bool{
	for _, row := range(d.Rows) {
		if newrow.Equals(&row) {
			return false
		}
	}
	d.Rows = append(d.Rows, newrow)
	return true
}
