class MyClass:
    def __init__(self):
        self.steps = iter([
            {"step": 1,  "message": "This is the first step"},
            {"step": 2,  "message": "This is the second step"},
            {"step": 3,  "message": "This is the third step"},
            {"step": 4,  "message": "This is the fourth step"},
        ])

    def issue_handler(func):
        def wrapper(self, *args, **kwargs):
            #iter(self.steps)
            print(next(self.steps)["message"])
            res = func(self, *args, **kwargs)
            print("After method execution")
            return res
        return wrapper

    def process(self):
        self.say_hello()
        self.do_second_thing()
        self.say_hello()

    @issue_handler
    def say_hello(self):
        print("Hello!")

    @issue_handler
    def do_second_thing(self):
        print("Second thing")

obj = MyClass()
obj.process()