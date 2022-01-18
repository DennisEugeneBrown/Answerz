class DataMapRepo:
    def __init__(self, data_map):
        self.DATA_MAP = data_map

    def getAllGroupings(self, _element):
        _element = _element.lower()
        if _element not in self.DATA_MAP:
            print("FATAL ERROR. Missing mapping for _DataElement = ", _element)
            return
        else:
            mapped_element = self.DATA_MAP[_element]
        mapped_groupings = []
        for group in mapped_element["Groupings"]:
            mapped_groupings.append(group)
        return mapped_groupings

    def findGrouping(self, _element, _groupAction):
        _element = _element.lower()
        if _element not in self.DATA_MAP:
            print("FATAL ERROR. Missing mapping for _DataElement = ", _element)
            return
        else:
            mapped_element = self.DATA_MAP[_element]
        print('Group Action: ', _groupAction)
        mapped_grouping = None
        for group in mapped_element["Groupings"]:
            if group["name"].lower() == _groupAction.lower():
                mapped_grouping = group
                break
        if not mapped_grouping:
            for group in mapped_element["Groupings"]:
                if "default" not in group:
                    continue
                if group["default"]:
                    mapped_grouping = group
                    break
        return mapped_element, mapped_grouping

    def findMapping(self, _element, _aggregation):
        _element = _element.lower()
        if not _element in self.DATA_MAP:
            print("FATAL ERROR. Missing mapping for _DataElement = ", _element, "... Using Calls.")
            mapped_element = self.DATA_MAP['Calls']
        else:
            mapped_element = self.DATA_MAP[_element]
        mapped_aggregation = None
        for agg in mapped_element["Aggregations"]:
            if agg["name"] == _aggregation:
                mapped_aggregation = agg
                break
        if not mapped_aggregation:
            for agg in mapped_element["Aggregations"]:
                if "default" not in agg:
                    continue
                if agg["default"]:
                    mapped_aggregation = agg
                    break
        return mapped_element, mapped_aggregation
