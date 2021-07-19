class ClassGetter(object):
    def __call__(self, module, class_name):
        __class = getattr(module, class_name)
        return __class()