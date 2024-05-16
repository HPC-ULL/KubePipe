from sklearn.model_selection import ParameterGrid
from sklearn.base import clone

class KubeGridSearch:
    def __init__(self, estimator, param_grid):
        self.estimator = estimator
        self.param_grid = param_grid

    def generate_estimators(self):
        for params in ParameterGrid(self.param_grid):
            new_estimator = clone(self.estimator)
            new_estimator.set_params(**params)
            yield new_estimator
