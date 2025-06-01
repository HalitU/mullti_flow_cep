import sys


class MeanStatBase:
    def __init__(self) -> None:
        self.count: float = 0.0
        self.mean: float = 0.0

    def add(self, ins: float) -> None:
        if ins == 0:
            return

        if ins <= 0:
            print(f"[FATAL ERROR] ANY VALUE SHOULD NOT BE BELOW 0!!!!")
            sys.exit(0)
            raise Exception("Incoming value cannot be below 0!")

        if self.count == 0:
            self.count = 1.0
            self.mean = ins
        else:
            self.count += 1.0
            self.mean = self.mean + (ins - self.mean) * 1.0 / self.count

    def add_multiple(self, mul_mean: float, mul_count: float):
        if (self.count + mul_count) == 0 or (self.mean + mul_mean == 0):
            return
        self.mean = (mul_mean * mul_count + self.count * self.mean) / (
            mul_count + self.count
        )
        self.count += mul_count

        if self.mean < 0 or self.count < 0:
            raise Exception("add multiple results cannot be below 0!!!")

    def total(self):
        return self.mean * self.count

    def reset(self):
        self.count = 0
        self.mean = 0

    def print_repr(self):
        return str(self.mean) + " " + str(self.count)
