package net.yetibyte.snowstorm;

public class Join {
	
	// Fields
	
	private JoinTypes _joinType = JoinTypes.Inner;
	private boolean _isEqui = true;
	
	private String _targetTable;
	private String _targetColumn;
	
	private String _sourceColumn;
	
	// Constructor
	
	public Join(JoinTypes joinType, String targetTable, String sourceColumn, String targetColumn) {
		
		_joinType = joinType;
		_targetTable = targetTable;
		_sourceColumn = sourceColumn;
		_targetColumn = targetColumn;
		
	}
	
	public Join(String targetTable, String sourceColumn, String targetColumn) {
		this(JoinTypes.Inner, targetTable, sourceColumn, targetColumn);
	}

	// Getters / Setters
	
	public JoinTypes getJoinType() {
		return _joinType;
	}

	public void setJoinType(JoinTypes joinType) {
		_joinType = joinType;
	}

	public boolean isEqui() {
		return _isEqui;
	}

	public void setEqui(boolean isEqui) {
		_isEqui = isEqui;
	}

	public String getTargetTable() {
		return _targetTable;
	}

	public void setTargetTable(String targetTable) {
		_targetTable = targetTable;
	}

	public String getTargetColumn() {
		return _targetColumn;
	}

	public void setTargetColumn(String targetColumn) {
		_targetColumn = targetColumn;
	}
	
	// Methods
	
	public String typeAsString() {
		
		return _joinType.toString().toUpperCase();
		
	}
	
	public String getClause() {
		
		return String.format("%s JOIN %s ON %s %s %s", typeAsString(), _targetTable, _sourceColumn, (_isEqui ? "=" : "<>"), _targetColumn);
		
	}
	
	public boolean isValid() {
		
		return DatasetAttributes.isSafeAttributeName(_targetTable)
				&& DatasetAttributes.isSafeAttributeName(_sourceColumn)
				&& DatasetAttributes.isSafeAttributeName(_targetColumn);
		
	}

}
