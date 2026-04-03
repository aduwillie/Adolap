use crate::{aggregate::AggFunc, logical_plan::{JoinSpec, LimitSpec, LogicalPlan, OrderBy}, predicate::Expr};

pub struct AQLBuilder {
  from: Option<String>,
  select: Vec<String>,
  predicate: Option<Expr>,
  joins: Vec<JoinSpec>,
  group_keys: Vec<String>,
  agg: Option<(AggFunc, String)>,
  group_predicate: Option<Expr>,
  order_by: Vec<OrderBy>,
  limit: Option<LimitSpec>,
}

pub fn aql() -> AQLBuilder {
  AQLBuilder {
    from: None,
    select: Vec::new(),
    predicate: None,
    joins: Vec::new(),
    group_keys: Vec::new(),
    agg: None,
    group_predicate: None,
    order_by: Vec::new(),
    limit: None,
  }
}

impl AQLBuilder {
  pub fn from(mut self, table: &str) -> Self {
    if self.from.is_some() {
      panic!("FROM clause already specified");
    }
    self.from = Some(table.to_string());
    self
  }

    pub fn select<S: Into<String>>(mut self, columns: impl IntoIterator<Item = S>) -> Self {
      let cols: Vec<String> = columns.into_iter().map(Into::into).collect();
      self.select.extend(cols);
      self
    }

    pub fn filter(mut self, predicate: Expr) -> Self {
      self.predicate = Some(match self.predicate.take() {
        None => predicate,
        Some(existing) => existing.and(predicate),
      });
      self
    }

   pub fn join(mut self, right_table_ref: &str, left_on: &str, right_on: &str) -> Self {
      self.joins.push(JoinSpec {
          right_table_ref: right_table_ref.to_string(),
          left_on: left_on.to_string(),
          right_on: right_on.to_string(),
      });
      self
    }

   pub fn group_by<S: Into<String>>(mut self, keys: impl IntoIterator<Item = S>) -> Self {
      let keys: Vec<String> = keys.into_iter().map(Into::into).collect();
      if !self.group_keys.is_empty() {
          panic!("GROUP BY already specified");
      }
      self.group_keys = keys;
      self
    }

    pub fn agg(mut self, func: AggFunc, column: &str) -> Self {
      if self.group_keys.is_empty() {
          panic!("AGG cannot be used without GROUP BY");
      }
      if self.agg.is_some() {
          panic!("AGG already specified");
      }
      self.agg = Some((func, column.to_string()));
      self
    }

      pub fn group_filter(mut self, predicate: Expr) -> Self {
        if self.group_keys.is_empty() {
          panic!("GROUP FILTER cannot be used without GROUP BY");
        }
        self.group_predicate = Some(match self.group_predicate.take() {
        None => predicate,
        Some(existing) => existing.and(predicate),
        });
        self
      }

      pub fn order_by(mut self, order_by: Vec<OrderBy>) -> Self {
        self.order_by = order_by;
        self
      }

      pub fn limit(mut self, limit: usize) -> Self {
        let spec = self.limit.get_or_insert(LimitSpec {
          limit: None,
          offset: 0,
        });
        spec.limit = Some(limit);
        self
      }

      pub fn offset(mut self, offset: usize) -> Self {
        let spec = self.limit.get_or_insert(LimitSpec {
          limit: None,
          offset: 0,
        });
        spec.offset = offset;
        self
      }

    pub fn build(self) -> LogicalPlan {
      let mut plan = LogicalPlan::scan(
          self.from.as_ref().expect("FROM clause is required")
      );

      for join in self.joins {
        plan = plan.join(
          LogicalPlan::scan(&join.right_table_ref),
          join.left_on,
          join.right_on,
        );
      }

      if let Some(pred) = self.predicate {
          plan = plan.filter(pred);
      }

      if !self.group_keys.is_empty() {
          let (func, col) = self.agg.expect("AGG required after GROUP BY");
          plan = plan.aggregate(self.group_keys, col, func);
          if let Some(pred) = self.group_predicate {
            plan = plan.group_filter(pred);
          }
        }

        if !self.select.is_empty() {
          plan = plan.project(self.select);
        }

        if !self.order_by.is_empty() {
          plan = plan.sort(self.order_by);
      }

      if let Some(limit) = self.limit {
        plan = plan.limit(limit.limit, limit.offset);
      }

      plan
    }
}
