package types

import (
	"fmt"
	"math"
	"strconv"

	"time"
)

type Time struct {
	time.Time
}

func (t Time) MarshalRQL() (interface{}, error) {
	return map[string]interface{}{
		"$REQL_TYPE": "TIME",
		"timestamp":  t.Unix(),
		"timezone":   "+00:00",
	}, nil
}

func (t *Time) UnmarshalRQL(data interface{}) error {
	obj, ok := data.(map[string]interface{})
	if !ok {
		return fmt.Errorf("Could not unmarshal time, expected a map but received %t", data)
	}

	timestamp := obj["epoch_time"].(float64)
	timezone := obj["timezone"].(string)

	sec, ms := math.Modf(timestamp)

	t.Time = time.Unix(int64(sec), int64(ms*1000*1000*1000))

	// Caclulate the timezone
	if timezone != "" {
		hours, err := strconv.Atoi(timezone[1:3])
		if err != nil {
			return err
		}
		minutes, err := strconv.Atoi(timezone[4:6])
		if err != nil {
			return err
		}
		tzOffset := ((hours * 60) + minutes) * 60
		if timezone[:1] == "-" {
			tzOffset = 0 - tzOffset
		}

		t.Time = t.In(time.FixedZone(timezone, tzOffset))
	}

	return nil
}
