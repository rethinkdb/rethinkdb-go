package gorethink

import "fmt"

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
