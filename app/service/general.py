import json

class SiteGeneralFunctions():
    @classmethod
    def get_age_range(self, val, rhsh):
        range_list = [[0,5],[6,10,],[11,15], [15,19], [20,30], [31,39], [40,49], [50,60], [60,100],[101,150]]
        for i in range_list:
            if i[0] <= val <= i[1]:
                rhsh["lo"] = i[0]
                rhsh["hi"] = i[1]
                return True
        return False

