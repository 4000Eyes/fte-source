
class SiteGeneralFunctions():
    def get_age_range(self, val, rhsh):
        range_list = [[0,5],[6,10,],[11,15], [15,19], [20,30], [31,39], [40,49], [50,60], [60,100]]
        for i in range_list:
            r = range(i[0], i[1])
            if val in r:
                rhsh["lo"] = i[0]
                rhsh["hi"] = i[1]
                return True
        return False
