import json
import enum
from typing import Dict, Any, List

class DistributionType(enum.Enum):
    UNIFORM = "uniform"
    NORMAL = "normal"
    EXPONENTIAL = "exponential"

class VariableDefinition:
    def __init__(self, name: str, distribution: DistributionType, parameters: Dict[str, float]):
        self.name = name
        self.distribution = distribution
        self.parameters = parameters
    
    def to_dict(self):
        return {
            "name": self.name,
            "distribution": self.distribution.value,
            "parameters": self.parameters
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]):
        return cls(
            name=data["name"],
            distribution=DistributionType(data["distribution"]),
            parameters=data["parameters"]
        )

class MonteCarloModel:
    def __init__(self, model_id: str, function_code: str, variables: List[VariableDefinition], iterations: int = 1000):
        self.model_id = model_id
        self.function_code = function_code
        self.variables = variables
        self.iterations = iterations
    
    def to_json(self):
        return json.dumps({
            "model_id": self.model_id,
            "function_code": self.function_code,
            "variables": [var.to_dict() for var in self.variables],
            "iterations": self.iterations
        })
    
    @classmethod
    def from_json(cls, json_str: str):
        data = json.loads(json_str)
        variables = [VariableDefinition.from_dict(var_data) for var_data in data["variables"]]
        return cls(
            model_id=data["model_id"],
            function_code=data["function_code"],
            variables=variables,
            iterations=data.get("iterations", 1000)
        )

class Scenario:
    def __init__(self, scenario_id: str, model_id: str, parameters: Dict[str, float]):
        self.scenario_id = scenario_id
        self.model_id = model_id
        self.parameters = parameters
    
    def to_json(self):
        return json.dumps({
            "scenario_id": self.scenario_id,
            "model_id": self.model_id,
            "parameters": self.parameters
        })
    
    @classmethod
    def from_json(cls, json_str: str):
        data = json.loads(json_str)
        return cls(
            scenario_id=data["scenario_id"],
            model_id=data["model_id"],
            parameters=data["parameters"]
        )

class Result:
    def __init__(self, scenario_id: str, model_id: str, result: float, worker_id: str):
        self.scenario_id = scenario_id
        self.model_id = model_id
        self.result = result
        self.worker_id = worker_id
    
    def to_json(self):
        return json.dumps({
            "scenario_id": self.scenario_id,
            "model_id": self.model_id,
            "result": self.result,
            "worker_id": self.worker_id
        })
    
    @classmethod
    def from_json(cls, json_str: str):
        data = json.loads(json_str)
        return cls(
            scenario_id=data["scenario_id"],
            model_id=data["model_id"],
            result=data["result"],
            worker_id=data["worker_id"]
        )