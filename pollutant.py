# coding=utf-8


class Pollutant(object):

    def __init__(self, name, pid, model_opt="DEFAULT"):
        self.name = name
        self.pid = pid
        self.model_opt = model_opt
        self.extra_property = {}

class BC(Pollutant):

    def __init__(self):
        super(BC, self).__init__("PM", "PM10")


class CO(Pollutant):

    def __init__(self):
        super(CO, self).__init__("CO", "CO")


class NO2(Pollutant):

    def __init__(self):
        super(NO2, self).__init__("NO", "NO2", "OLM")
        self.extra_property['OZONEVAL'] = '09 PPB'
        self.extra_property['NO2EQUIL'] = '0.50'


class PM(Pollutant):

    def __init__(self):
        super(PM, self).__init__("PM", "PM10")
