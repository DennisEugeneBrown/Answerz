from answerz.model.DataMapRepo import DataMapRepo


class EntityDecoderBase:
    def lookupTablesAndField(self, _element, _aggregation, entity_name, data_map):
        self.data_map = data_map

        data_map_repo = DataMapRepo(data_map)
        mapped_element, mapped_aggregation = data_map_repo.findMapping(
            _element, _aggregation)
        for dim in mapped_element["Dimensions"]:
            if dim["name"] == entity_name:
                tables = mapped_aggregation["tables"]
                # No handling yet for additional table requirements on field

                result = dim
                result["tables"] = tables
                return result

        return None

    def lookupTablesAndFieldByType(self, _element, _aggregation, entity_type, data_map):
        self.data_map = data_map

        data_map_repo = DataMapRepo(self.data_map)
        mapped_element, mapped_aggregation = data_map_repo.findMapping(
            _element, _aggregation)
        for dim in mapped_element["Dimensions"]:
            if dim["type"] == entity_type:
                tables = mapped_aggregation["tables"]
                # No handling yet for additional table requirements on field

                result = dim
                result["tables"] = tables
                return result

        return None
