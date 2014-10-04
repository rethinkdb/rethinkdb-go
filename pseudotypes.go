package gorethink

import (
	"encoding/base64"
	"math"
	"strconv"
	"time"

	"fmt"
)

// Pseudo-Type structs
type Geometry struct {
	Type  string
	Point Point
	Line  Line
	Lines Lines
}

type Point LatLon
type Line []Point
type Lines []Line
type LatLon struct {
	Lat, Lon float64
}

func convertPseudotype(obj map[string]interface{}, opts map[string]interface{}) (interface{}, error) {
	if reqlType, ok := obj["$reql_type$"]; ok {
		if reqlType == "TIME" {
			// load timeFormat, set to native if the option was not set
			timeFormat := "native"
			if opt, ok := opts["time_format"]; ok {
				if sopt, ok := opt.(string); ok {
					timeFormat = sopt
				} else {
					return nil, fmt.Errorf("Invalid time_format run option \"%s\".", opt)
				}
			}

			if timeFormat == "native" {
				return reqlTimeToNativeTime(obj["epoch_time"].(float64), obj["timezone"].(string))
			} else if timeFormat == "raw" {
				return obj, nil
			} else {
				return nil, fmt.Errorf("Unknown time_format run option \"%s\".", reqlType)
			}
		} else if reqlType == "GROUPED_DATA" {
			// load groupFormat, set to native if the option was not set
			groupFormat := "native"
			if opt, ok := opts["group_format"]; ok {
				if sopt, ok := opt.(string); ok {
					groupFormat = sopt
				} else {
					return nil, fmt.Errorf("Invalid group_format run option \"%s\".", opt)
				}
			}

			if groupFormat == "native" {
				return reqlGroupedDataToObj(obj)
			} else if groupFormat == "raw" {
				return obj, nil
			} else {
				return nil, fmt.Errorf("Unknown group_format run option \"%s\".", reqlType)
			}
		} else if reqlType == "BINARY" {
			binaryFormat := "native"
			if opt, ok := opts["binary_format"]; ok {
				if sopt, ok := opt.(string); ok {
					binaryFormat = sopt
				} else {
					return nil, fmt.Errorf("Invalid binary_format run option \"%s\".", opt)
				}
			}

			if binaryFormat == "native" {
				return reqlBinaryToNativeBytes(obj)
			} else if binaryFormat == "raw" {
				return obj, nil
			} else {
				return nil, fmt.Errorf("Unknown binary_format run option \"%s\".", reqlType)
			}
		} else if reqlType == "GEOMETRY" {
			geometryFormat := "native"
			if opt, ok := opts["geometry_format"]; ok {
				if sopt, ok := opt.(string); ok {
					geometryFormat = sopt
				} else {
					return nil, fmt.Errorf("Invalid geometry_format run option \"%s\".", opt)
				}
			}

			if geometryFormat == "native" {
				return reqlGeometryToNativeGeometry(obj)
			} else if geometryFormat == "raw" {
				return obj, nil
			} else {
				return nil, fmt.Errorf("Unknown geometry_format run option \"%s\".", reqlType)
			}
		} else {
			return obj, nil
		}
	}

	return obj, nil
}

func recursivelyConvertPseudotype(obj interface{}, opts map[string]interface{}) (interface{}, error) {
	var err error

	switch obj := obj.(type) {
	case []interface{}:
		for key, val := range obj {
			obj[key], err = recursivelyConvertPseudotype(val, opts)
			if err != nil {
				return nil, err
			}
		}
	case map[string]interface{}:
		for key, val := range obj {
			obj[key], err = recursivelyConvertPseudotype(val, opts)
			if err != nil {
				return nil, err
			}
		}

		pobj, err := convertPseudotype(obj, opts)
		if err != nil {
			return nil, err
		}

		return pobj, nil
	}

	return obj, nil
}

// Pseudo-type helper functions

func reqlTimeToNativeTime(timestamp float64, timezone string) (time.Time, error) {
	sec, ms := math.Modf(timestamp)

	t := time.Unix(int64(sec), int64(ms*1000*1000*1000))

	// Caclulate the timezone
	if timezone != "" {
		hours, err := strconv.Atoi(timezone[1:3])
		if err != nil {
			return time.Time{}, err
		}
		minutes, err := strconv.Atoi(timezone[4:6])
		if err != nil {
			return time.Time{}, err
		}
		tzOffset := ((hours * 60) + minutes) * 60
		if timezone[:1] == "-" {
			tzOffset = 0 - tzOffset
		}

		t = t.In(time.FixedZone(timezone, tzOffset))
	}

	return t, nil
}

func reqlGroupedDataToObj(obj map[string]interface{}) (interface{}, error) {
	if data, ok := obj["data"]; ok {
		ret := []interface{}{}
		for _, v := range data.([]interface{}) {
			v := v.([]interface{})
			ret = append(ret, map[string]interface{}{
				"group":     v[0],
				"reduction": v[1],
			})
		}
		return ret, nil
	} else {
		return nil, fmt.Errorf("pseudo-type GROUPED_DATA object %v does not have the expected field \"data\"", obj)
	}
}

func reqlBinaryToNativeBytes(obj map[string]interface{}) (interface{}, error) {
	if data, ok := obj["data"]; ok {
		if data, ok := data.(string); ok {
			b, err := base64.StdEncoding.DecodeString(data)
			if err != nil {
				return nil, fmt.Errorf("error decoding pseudo-type BINARY object %v", obj)
			}

			return b, nil
		} else {
			return nil, fmt.Errorf("pseudo-type BINARY object %v field \"data\" is not valid", obj)
		}
	} else {
		return nil, fmt.Errorf("pseudo-type BINARY object %v does not have the expected field \"data\"", obj)
	}
}

func reqlGeometryToNativeGeometry(obj map[string]interface{}) (interface{}, error) {
	if typ, ok := obj["type"]; !ok {
		return nil, fmt.Errorf("pseudo-type GEOMETRY object %v does not have the expected field \"type\"", obj)
	} else if typ, ok := typ.(string); !ok {
		return nil, fmt.Errorf("pseudo-type GEOMETRY object %v field \"type\" is not valid", obj)
	} else if coords, ok := obj["coordinates"]; !ok {
		return nil, fmt.Errorf("pseudo-type GEOMETRY object %v does not have the expected field \"coordinates\"", obj)
	} else if typ == "Point" {
		if point, err := unmarshalPoint(coords); err != nil {
			return nil, err
		} else {
			return Geometry{
				Type:  "Point",
				Point: point,
			}, nil
		}
	} else if typ == "LineString" {
		if line, err := unmarshalLineString(coords); err != nil {
			return nil, err
		} else {
			return Geometry{
				Type: "LineString",
				Line: line,
			}, nil
		}
	} else if typ == "Polygon" {
		if lines, err := unmarshalPolygon(coords); err != nil {
			return nil, err
		} else {
			return Geometry{
				Type:  "Polygon",
				Lines: lines,
			}, nil
		}
	} else {
		return nil, fmt.Errorf("pseudo-type GEOMETRY object %v field has unknown type %s", typ)
	}
}

func unmarshalPoint(v interface{}) (Point, error) {
	coords, ok := v.([]interface{})
	if !ok {
		return Point{}, fmt.Errorf("pseudo-type GEOMETRY object %v field \"coordinates\" is not valid", v)
	}
	if len(coords) != 2 {
		return Point{}, fmt.Errorf("pseudo-type GEOMETRY object %v field \"coordinates\" is not valid", v)
	}
	lat, ok := coords[0].(float64)
	if !ok {
		return Point{}, fmt.Errorf("pseudo-type GEOMETRY object %v field \"coordinates\" is not valid", v)
	}
	lon, ok := coords[1].(float64)
	if !ok {
		return Point{}, fmt.Errorf("pseudo-type GEOMETRY object %v field \"coordinates\" is not valid", v)
	}

	return Point{lat, lon}, nil
}

func unmarshalLineString(v interface{}) (Line, error) {
	points, ok := v.([]interface{})
	if !ok {
		return Line{}, fmt.Errorf("pseudo-type GEOMETRY object %v field \"coordinates\" is not valid", v)
	}

	var err error
	line := make(Line, len(points))
	for i, coords := range points {
		line[i], err = unmarshalPoint(coords)
		if err != nil {
			return Line{}, err
		}
	}
	return line, nil
}

func unmarshalPolygon(v interface{}) (Lines, error) {
	lines, ok := v.([]interface{})
	if !ok {
		return Lines{}, fmt.Errorf("pseudo-type GEOMETRY object %v field \"coordinates\" is not valid", v)
	}

	var err error
	polygon := make(Lines, len(lines))
	for i, line := range lines {
		polygon[i], err = unmarshalLineString(line)
		if err != nil {
			return Lines{}, err
		}
	}
	return polygon, nil
}
