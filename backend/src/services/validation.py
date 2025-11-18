"""
Data Validation Module - Fully Dynamic and Metadata-Driven
All validation logic is loaded from metadata, not hardcoded
"""

from typing import Dict, List, Tuple, Any, Callable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from loguru import logger
import re


class ValidationRegistry:
    """Registry for validation functions - extensible via metadata"""
    
    def __init__(self):
        self._validators: Dict[str, Callable] = {}
        self._register_built_in_validators()
    
    def _register_built_in_validators(self):
        """Register built-in validation functions"""
        self.register("notNull", self._not_null)
        self.register("notEmpty", self._not_empty)
        self.register("isNumeric", self._is_numeric)
        self.register("rangeCheck", self._range_check)
        self.register("regex", self._regex_match)
        self.register("email", self._email_format)
        self.register("length", self._length_check)
        self.register("inList", self._in_list)
        self.register("custom", self._custom_expression)
    
    def register(self, name: str, validator: Callable):
        """Register a validation function"""
        self._validators[name] = validator
        logger.debug(f"Registered validator: {name}")
    
    def get(self, name: str) -> Callable:
        """Get a validation function by name"""
        if name not in self._validators:
            raise ValueError(f"Unknown validator: {name}")
        return self._validators[name]
    
    def list_validators(self) -> List[str]:
        """List all available validators"""
        return list(self._validators.keys())
    
    # Built-in validator implementations
    def _not_null(self, df: DataFrame, field: str, params: Dict = None) -> F.Column:
        """Field must not be null"""
        error_msg = params.get("error_message", f"{field} cannot be null") if params else f"{field} cannot be null"
        return F.when(F.col(field).isNull(), F.lit(error_msg))
    
    def _not_empty(self, df: DataFrame, field: str, params: Dict = None) -> F.Column:
        """Field must not be empty string"""
        error_msg = params.get("error_message", f"{field} cannot be empty") if params else f"{field} cannot be empty"
        return F.when(
            (F.col(field).isNull()) | (F.trim(F.col(field)) == ""),
            F.lit(error_msg)
        )
    
    def _is_numeric(self, df: DataFrame, field: str, params: Dict = None) -> F.Column:
        """Field must be numeric"""
        error_msg = params.get("error_message", f"{field} must be numeric") if params else f"{field} must be numeric"
        return F.when(
            F.col(field).cast("double").isNull() & F.col(field).isNotNull(),
            F.lit(error_msg)
        )
    
    def _range_check(self, df: DataFrame, field: str, params: Dict = None) -> F.Column:
        """Field must be within specified range"""
        if not params:
            raise ValueError("rangeCheck requires 'min' and 'max' parameters")
        
        min_val = params.get("min", float('-inf'))
        max_val = params.get("max", float('inf'))
        error_msg = params.get("error_message", f"{field} must be between {min_val} and {max_val}")
        
        return F.when(
            (F.col(field) < min_val) | (F.col(field) > max_val),
            F.lit(error_msg)
        )
    
    def _regex_match(self, df: DataFrame, field: str, params: Dict = None) -> F.Column:
        """Field must match regex pattern"""
        if not params or "pattern" not in params:
            raise ValueError("regex validator requires 'pattern' parameter")
        
        pattern = params["pattern"]
        error_msg = params.get("error_message", f"{field} does not match required pattern")
        
        return F.when(
            ~F.col(field).rlike(pattern) & F.col(field).isNotNull(),
            F.lit(error_msg)
        )
    
    def _email_format(self, df: DataFrame, field: str, params: Dict = None) -> F.Column:
        """Field must be valid email format"""
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        error_msg = params.get("error_message", f"{field} must be valid email") if params else f"{field} must be valid email"
        
        return F.when(
            ~F.col(field).rlike(email_pattern) & F.col(field).isNotNull(),
            F.lit(error_msg)
        )
    
    def _length_check(self, df: DataFrame, field: str, params: Dict = None) -> F.Column:
        """Field length must be within specified range"""
        if not params:
            raise ValueError("length validator requires 'min' or 'max' parameter")
        
        min_len = params.get("min", 0)
        max_len = params.get("max", float('inf'))
        error_msg = params.get("error_message", f"{field} length must be between {min_len} and {max_len}")
        
        return F.when(
            (F.length(F.col(field)) < min_len) | (F.length(F.col(field)) > max_len),
            F.lit(error_msg)
        )
    
    def _in_list(self, df: DataFrame, field: str, params: Dict = None) -> F.Column:
        """Field value must be in allowed list"""
        if not params or "values" not in params:
            raise ValueError("inList validator requires 'values' parameter")
        
        allowed_values = params["values"]
        error_msg = params.get("error_message", f"{field} must be one of {allowed_values}")
        
        return F.when(
            ~F.col(field).isin(allowed_values) & F.col(field).isNotNull(),
            F.lit(error_msg)
        )
    
    def _custom_expression(self, df: DataFrame, field: str, params: Dict = None) -> F.Column:
        """Custom SQL expression validation"""
        if not params or "expression" not in params:
            raise ValueError("custom validator requires 'expression' parameter")
        
        expression = params["expression"]
        error_msg = params.get("error_message", f"{field} failed custom validation")
        
        return F.when(
            ~F.expr(expression.replace("{field}", field)),
            F.lit(error_msg)
        )


class DataValidator:
    """Fully dynamic data validator - operates entirely from metadata"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.registry = ValidationRegistry()
    
    def validate_dataframe(
        self, 
        df: DataFrame, 
        validation_metadata: Dict
    ) -> Tuple[DataFrame, DataFrame]:
        """
        Validate dataframe based on metadata configuration
        
        Args:
            df: Input dataframe
            validation_metadata: Complete validation metadata from config
            
        Returns:
            Tuple of (valid_df, invalid_df)
        """
        validations = validation_metadata.get("validations", [])
        
        if not validations:
            logger.warning("No validations defined in metadata")
            return df, self.spark.createDataFrame([], df.schema)
        
        logger.info(f"Starting validation on {df.count()} records with {len(validations)} rules")
        

        validated_df = df
        validation_columns = []
        
        for validation_config in validations:
            field = validation_config["field"]
            rules = validation_config.get("validations", [])
            

            for rule in rules:
                if isinstance(rule, str):
                    rule_name = rule
                    rule_params = validation_config.copy()
                elif isinstance(rule, dict):
                    rule_name = rule.get("type")
                    rule_params = rule.get("params", {})
                    rule_params.update(validation_config)
                else:
                    logger.warning(f"Invalid rule format: {rule}")
                    continue
                

                validator = self.registry.get(rule_name)
                

                col_name = f"_val_{field}_{rule_name}"
                validation_expr = validator(validated_df, field, rule_params)
                validated_df = validated_df.withColumn(col_name, validation_expr)
                validation_columns.append(col_name)
        
        # Aggregate all validation errors into a single map
        validated_df = self._create_validation_errors_map(validated_df, validation_columns, validations)
        
        # Split into valid and invalid
        valid_df = validated_df.filter(F.col("_validation_error_count") == 0)
        invalid_df = validated_df.filter(F.col("_validation_error_count") > 0)
        
        # Clean up valid records
        valid_df = valid_df.drop("_validation_error_count", "validation_errors")
        
        # Keep only original columns + validation_errors for invalid records
        original_columns = df.columns
        invalid_df = invalid_df.select(*original_columns, "validation_errors")
        
        logger.info(f"Validation complete: {valid_df.count()} valid, {invalid_df.count()} invalid")
        
        return valid_df, invalid_df
    
    def _create_validation_errors_map(
        self, 
        df: DataFrame, 
        validation_columns: List[str],
        validations: List[Dict]
    ) -> DataFrame:
        """Create aggregated validation errors map - FIXED VERSION"""

        # Build field -> column mapping
        field_to_cols = {}
        for val_col in validation_columns:
            # Extract field name from column name pattern: _val_{field}_{rule}
            parts = val_col.split("_")
            if len(parts) >= 3:
                field = "_".join(parts[2:-1])
                if field not in field_to_cols:
                    field_to_cols[field] = []
                field_to_cols[field].append(val_col)


        map_keys = []
        map_values = []

        for field, cols in field_to_cols.items():

            error_expr = F.coalesce(*[F.col(col) for col in cols])
            map_keys.append(F.lit(field))
            map_values.append(error_expr)


        if map_keys:

            keys_array = F.array(*map_keys)
            values_array = F.array(*map_values)


            df = df.withColumn("_temp_map", F.map_from_arrays(keys_array, values_array))


            df = df.withColumn(
                "validation_errors",
                F.expr("map_filter(_temp_map, (k, v) -> v is not null)")
            )
            
            # Count errors
            df = df.withColumn(
                "_validation_error_count",
                F.size(F.col("validation_errors"))
            )
            
            df = df.drop("_temp_map")
        else:
            df = df.withColumn("validation_errors", F.create_map())
            df = df.withColumn("_validation_error_count", F.lit(0))
        

        df = df.drop(*validation_columns)
        
        return df
    
    def register_custom_validator(self, name: str, validator: Callable):
        """Allow users to register custom validators via code or plugins"""
        self.registry.register(name, validator)
    
    def get_available_validators(self) -> List[str]:
        """Get list of all available validators"""
        return self.registry.list_validators()