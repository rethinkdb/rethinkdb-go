package types

type Geometry struct {
	Type  string
	Point Point
	Line  Line
	Lines Lines
}

type Point struct {
	Lat, Lon float64
}
type Line []Point
type Lines []Line
