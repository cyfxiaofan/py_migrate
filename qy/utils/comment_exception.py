# 自定义异常

class CustomError(Exception):
    def __init__(self, error):
        super().__init__(self)
        self.errorinfo = error

    def __str__(self):
        return self.errorinfo
