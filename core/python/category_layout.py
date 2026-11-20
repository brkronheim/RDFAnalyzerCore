"""Portable named category layouts, shared with CategoryLayout.h indexing."""
import math


class CategoryLayout:
    def __init__(self, axes, *, base=0):
        self.axes = list(axes)
        self.base = base
        names = [axis["name"] for axis in self.axes]
        self.sizes = [axis["size"] for axis in self.axes]
        if (not names or len(set(names)) != len(names) or base not in (0,1) or
                any(type(size) is not int or size < 1 for size in self.sizes)):
            raise ValueError("category axes require unique names and positive sizes")

    def encode(self, values):
        if len(values) != len(self.axes):
            raise ValueError("category coordinate count mismatch")
        index = 0
        for value, size in zip(values, self.sizes):
            if type(value) is not int or not 0 <= value < size:
                raise ValueError("category coordinate outside layout")
            index = index*size+value
        return index+self.base

    def decode(self, index):
        if type(index) is not int or not self.base <= index < self.base+math.prod(self.sizes):
            raise ValueError("category index outside layout")
        index -= self.base
        values = []
        for size in reversed(self.sizes):
            values.append(index % size)
            index //= size
        return dict(zip((axis["name"] for axis in self.axes), reversed(values)))

    def metadata(self):
        return dict(schema="rdf-category-layout-v1", axes=self.axes, id_base=self.base,
                    flatten_order=[axis["name"] for axis in self.axes])

    @classmethod
    def from_metadata(cls, record):
        if record.get("schema") != "rdf-category-layout-v1":
            raise ValueError("unsupported category layout")
        layout = cls(record["axes"], base=record["id_base"])
        if record.get("flatten_order") != [axis["name"] for axis in layout.axes]:
            raise ValueError("category flatten order disagrees with axis order")
        return layout
