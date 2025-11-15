"""
Data Transformation Module - Fully Dynamic and Metadata-Driven
All transformations are loaded and executed from metadata, not hardcoded
"""

from typing import Dict, List, Any, Callable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from loguru import logger


class TransformationRegistry:
    """Registry for transformation functions - fully extensible"""
    
    def __init__(self):
        self._transformations: Dict[str, Callable] = {}
        self._register_built_in_transformations()
    
    def _register_built_in_transformations(self):
        """Register built-in transformation functions"""
        self.register("current_timestamp", self._current_timestamp)
        self.register("current_date", self._current_date)
        self.register("literal", self._literal)
        self.register("uuid", self._uuid)
        self.register("hash", self._hash)
        self.register("concat", self._concat)
        self.register("substring", self._substring)
        self.register("upper", self._upper)
        self.register("lower", self._lower)
        self.register("trim", self._trim)
        self.register("cast", self._cast)
        self.register("coalesce", self._coalesce)
        self.register("when", self._when)
        self.register("year", self._year)
        self.register("month", self._month)
        self.register("day", self._day)
        self.register("datediff", self._datediff)
        self.register("expr", self._expression)
        self.register("lookup", self._lookup)
    
    def register(self, name: str, transformation: Callable):
        """Register a transformation function"""
        self._transformations[name] = transformation
        logger.debug(f"Registered transformation: {name}")
    
    def get(self, name: str) -> Callable:
        """Get a transformation function by name"""
        if name not in self._transformations:
            raise ValueError(f"Unknown transformation: {name}")
        return self._transformations[name]
    
    def list_transformations(self) -> List[str]:
        """List all available transformations"""
        return list(self._transformations.keys())
    
    # Built-in transformation implementations
    def _current_timestamp(self, df: DataFrame, params: Dict) -> F.Column:
        """Add current timestamp"""
        return F.current_timestamp()
    
    def _current_date(self, df: DataFrame, params: Dict) -> F.Column:
        """Add current date"""
        return F.current_date()
    
    def _literal(self, df: DataFrame, params: Dict) -> F.Column:
        """Add literal value"""
        value = params.get("value")
        if value is None:
            raise ValueError("literal transformation requires 'value' parameter")
        return F.lit(value)
    
    def _uuid(self, df: DataFrame, params: Dict) -> F.Column:
        """Generate UUID"""
        return F.expr("uuid()")
    
    def _hash(self, df: DataFrame, params: Dict) -> F.Column:
        """Create hash from columns"""
        columns = params.get("columns", [])
        algorithm = params.get("algorithm", "sha256")
        
        if not columns:
            raise ValueError("hash transformation requires 'columns' parameter")
        
        concat_expr = F.concat_ws("||", *[F.col(col) for col in columns])
        
        if algorithm == "md5":
            return F.md5(concat_expr)
        elif algorithm == "sha1":
            return F.sha1(concat_expr)
        elif algorithm == "sha256":
            return F.sha2(concat_expr, 256)
        elif algorithm == "sha512":
            return F.sha2(concat_expr, 512)
        else:
            raise ValueError(f"Unknown hash algorithm: {algorithm}")
    
    def _concat(self, df: DataFrame, params: Dict) -> F.Column:
        """Concatenate columns"""
        columns = params.get("columns", [])
        separator = params.get("separator", "")
        
        if not columns:
            raise ValueError("concat transformation requires 'columns' parameter")
        
        if separator:
            return F.concat_ws(separator, *[F.col(col) for col in columns])
        return F.concat(*[F.col(col) for col in columns])
    
    def _substring(self, df: DataFrame, params: Dict) -> F.Column:
        """Extract substring"""
        column = params.get("column")
        start = params.get("start", 1)
        length = params.get("length")
        
        if not column:
            raise ValueError("substring transformation requires 'column' parameter")
        
        if length:
            return F.substring(F.col(column), start, length)
        return F.substring(F.col(column), start, 999999)
    
    def _upper(self, df: DataFrame, params: Dict) -> F.Column:
        """Convert to uppercase"""
        column = params.get("column")
        if not column:
            raise ValueError("upper transformation requires 'column' parameter")
        return F.upper(F.col(column))
    
    def _lower(self, df: DataFrame, params: Dict) -> F.Column:
        """Convert to lowercase"""
        column = params.get("column")
        if not column:
            raise ValueError("lower transformation requires 'column' parameter")
        return F.lower(F.col(column))
    
    def _trim(self, df: DataFrame, params: Dict) -> F.Column:
        """Trim whitespace"""
        column = params.get("column")
        if not column:
            raise ValueError("trim transformation requires 'column' parameter")
        return F.trim(F.col(column))
    
    def _cast(self, df: DataFrame, params: Dict) -> F.Column:
        """Cast column to type"""
        column = params.get("column")
        data_type = params.get("type")
        
        if not column or not data_type:
            raise ValueError("cast transformation requires 'column' and 'type' parameters")
        
        return F.col(column).cast(data_type)
    
    def _coalesce(self, df: DataFrame, params: Dict) -> F.Column:
        """Return first non-null value"""
        columns = params.get("columns", [])
        
        if not columns:
            raise ValueError("coalesce transformation requires 'columns' parameter")
        
        return F.coalesce(*[F.col(col) for col in columns])
    
    def _when(self, df: DataFrame, params: Dict) -> F.Column:
        """Conditional expression"""
        condition = params.get("condition")
        then_value = params.get("then")
        otherwise_value = params.get("otherwise")
        
        if not condition or then_value is None:
            raise ValueError("when transformation requires 'condition' and 'then' parameters")
        
        expr = F.when(F.expr(condition), F.lit(then_value))
        
        if otherwise_value is not None:
            expr = expr.otherwise(F.lit(otherwise_value))
        
        return expr
    
    def _year(self, df: DataFrame, params: Dict) -> F.Column:
        """Extract year from date"""
        column = params.get("column")
        if not column:
            raise ValueError("year transformation requires 'column' parameter")
        return F.year(F.col(column))
    
    def _month(self, df: DataFrame, params: Dict) -> F.Column:
        """Extract month from date"""
        column = params.get("column")
        if not column:
            raise ValueError("month transformation requires 'column' parameter")
        return F.month(F.col(column))
    
    def _day(self, df: DataFrame, params: Dict) -> F.Column:
        """Extract day from date"""
        column = params.get("column")
        if not column:
            raise ValueError("day transformation requires 'column' parameter")
        return F.day(F.col(column))
    
    def _datediff(self, df: DataFrame, params: Dict) -> F.Column:
        """Calculate date difference"""
        start_date = params.get("start_date")
        end_date = params.get("end_date")
        
        if not start_date or not end_date:
            raise ValueError("datediff transformation requires 'start_date' and 'end_date' parameters")
        
        return F.datediff(F.col(end_date), F.col(start_date))
    
    def _expression(self, df: DataFrame, params: Dict) -> F.Column:
        """Custom SQL expression"""
        expression = params.get("expression")
        
        if not expression:
            raise ValueError("expr transformation requires 'expression' parameter")
        
        return F.expr(expression)
    
    def _lookup(self, df: DataFrame, params: Dict) -> F.Column:
        """Lookup value from mapping"""
        column = params.get("column")
        mapping = params.get("mapping", {})
        default = params.get("default")
        
        if not column or not mapping:
            raise ValueError("lookup transformation requires 'column' and 'mapping' parameters")
        
        # Build when-otherwise chain
        expr = None
        for key, value in mapping.items():
            condition = F.col(column) == key
            if expr is None:
                expr = F.when(condition, F.lit(value))
            else:
                expr = expr.when(condition, F.lit(value))
        
        if default is not None:
            expr = expr.otherwise(F.lit(default))
        
        return expr


class DataTransformer:
    """Fully dynamic data transformer - operates entirely from metadata"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.registry = TransformationRegistry()
    
    def apply_transformations(
        self, 
        df: DataFrame, 
        transformation_metadata: Dict
    ) -> DataFrame:
        """
        Apply transformations based on metadata configuration
        
        Args:
            df: Input dataframe
            transformation_metadata: Transformation metadata from config
            
        Returns:
            Transformed dataframe
        """
        transform_type = transformation_metadata.get("type")
        params = transformation_metadata.get("params", {})
        
        logger.info(f"Applying transformation type: {transform_type}")
        
        if transform_type == "add_fields":
            return self._handle_add_fields(df, params)
        elif transform_type == "rename_columns":
            return self._handle_rename_columns(df, params)
        elif transform_type == "drop_columns":
            return self._handle_drop_columns(df, params)
        elif transform_type == "select_columns":
            return self._handle_select_columns(df, params)
        elif transform_type == "filter":
            return self._handle_filter(df, params)
        elif transform_type == "deduplicate":
            return self._handle_deduplicate(df, params)
        elif transform_type == "join":
            return self._handle_join(df, params)
        elif transform_type == "union":
            return self._handle_union(df, params)
        elif transform_type == "aggregate":
            return self._handle_aggregate(df, params)
        elif transform_type == "pivot":
            return self._handle_pivot(df, params)
        elif transform_type == "window":
            return self._handle_window(df, params)
        else:
            raise ValueError(f"Unknown transformation type: {transform_type}")
    
    def _handle_add_fields(self, df: DataFrame, params: Dict) -> DataFrame:
        """Add new fields dynamically"""
        add_fields = params.get("addFields", [])
        
        for field_config in add_fields:
            field_name = field_config["name"]
            function = field_config["function"]
            func_params = field_config.copy()
            
            # Get transformation from registry
            transformer = self.registry.get(function)
            
            # Apply transformation
            df = df.withColumn(field_name, transformer(df, func_params))
            logger.debug(f"Added field: {field_name} using {function}")
        
        return df
    
    def _handle_rename_columns(self, df: DataFrame, params: Dict) -> DataFrame:
        """Rename columns dynamically"""
        rename_map = params.get("mapping", {})
        
        for old_name, new_name in rename_map.items():
            if old_name in df.columns:
                df = df.withColumnRenamed(old_name, new_name)
                logger.debug(f"Renamed: {old_name} -> {new_name}")
        
        return df
    
    def _handle_drop_columns(self, df: DataFrame, params: Dict) -> DataFrame:
        """Drop columns dynamically"""
        columns = params.get("columns", [])
        existing = [col for col in columns if col in df.columns]
        
        if existing:
            df = df.drop(*existing)
            logger.debug(f"Dropped columns: {existing}")
        
        return df
    
    def _handle_select_columns(self, df: DataFrame, params: Dict) -> DataFrame:
        """Select specific columns"""
        columns = params.get("columns", [])
        existing = [col for col in columns if col in df.columns]
        
        return df.select(*existing)
    
    def _handle_filter(self, df: DataFrame, params: Dict) -> DataFrame:
        """Filter rows based on condition"""
        condition = params.get("condition")
        
        if not condition:
            raise ValueError("filter requires 'condition' parameter")
        
        return df.filter(condition)
    
    def _handle_deduplicate(self, df: DataFrame, params: Dict) -> DataFrame:
        """Remove duplicate rows"""
        subset = params.get("subset")
        
        if subset:
            return df.dropDuplicates(subset)
        return df.dropDuplicates()
    
    def _handle_join(self, df: DataFrame, params: Dict) -> DataFrame:
        """Join with another dataframe"""
        # This requires access to other dataframes - handle in pipeline
        raise NotImplementedError("Join transformation handled at pipeline level")
    
    def _handle_union(self, df: DataFrame, params: Dict) -> DataFrame:
        """Union with another dataframe"""
        # This requires access to other dataframes - handle in pipeline
        raise NotImplementedError("Union transformation handled at pipeline level")
    
    def _handle_aggregate(self, df: DataFrame, params: Dict) -> DataFrame:
        """Aggregate data"""
        group_by = params.get("groupBy", [])
        aggregations = params.get("aggregations", [])
        
        if not group_by or not aggregations:
            raise ValueError("aggregate requires 'groupBy' and 'aggregations' parameters")
        
        agg_exprs = []
        for agg in aggregations:
            func = agg["function"]
            column = agg["column"]
            alias = agg.get("alias", f"{func}_{column}")
            
            if func == "sum":
                agg_exprs.append(F.sum(column).alias(alias))
            elif func == "avg":
                agg_exprs.append(F.avg(column).alias(alias))
            elif func == "count":
                agg_exprs.append(F.count(column).alias(alias))
            elif func == "min":
                agg_exprs.append(F.min(column).alias(alias))
            elif func == "max":
                agg_exprs.append(F.max(column).alias(alias))
        
        return df.groupBy(*group_by).agg(*agg_exprs)
    
    def _handle_pivot(self, df: DataFrame, params: Dict) -> DataFrame:
        """Pivot data"""
        group_by = params.get("groupBy", [])
        pivot_column = params.get("pivotColumn")
        value_column = params.get("valueColumn")
        
        if not pivot_column or not value_column:
            raise ValueError("pivot requires 'pivotColumn' and 'valueColumn' parameters")
        
        return df.groupBy(*group_by).pivot(pivot_column).agg(F.first(value_column))
    
    def _handle_window(self, df: DataFrame, params: Dict) -> DataFrame:
        """Window function"""
        from pyspark.sql.window import Window
        
        partition_by = params.get("partitionBy", [])
        order_by = params.get("orderBy", [])
        function = params.get("function")
        column = params.get("column")
        alias = params.get("alias")
        
        if not function or not alias:
            raise ValueError("window requires 'function' and 'alias' parameters")
        
        window_spec = Window.partitionBy(*partition_by).orderBy(*order_by)
        
        if function == "row_number":
            df = df.withColumn(alias, F.row_number().over(window_spec))
        elif function == "rank":
            df = df.withColumn(alias, F.rank().over(window_spec))
        elif function == "dense_rank":
            df = df.withColumn(alias, F.dense_rank().over(window_spec))
        elif function == "lag":
            offset = params.get("offset", 1)
            df = df.withColumn(alias, F.lag(column, offset).over(window_spec))
        elif function == "lead":
            offset = params.get("offset", 1)
            df = df.withColumn(alias, F.lead(column, offset).over(window_spec))
        
        return df
    
    def register_custom_transformation(self, name: str, transformation: Callable):
        """Allow users to register custom transformations"""
        self.registry.register(name, transformation)
    
    def get_available_transformations(self) -> List[str]:
        """Get list of all available transformations"""
        return self.registry.list_transformations()