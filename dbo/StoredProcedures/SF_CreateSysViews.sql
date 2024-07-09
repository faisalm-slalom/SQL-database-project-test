Create PROCEDURE [dbo].[SF_CreateSysViews]
	@LinkedServer sysname
AS
declare @sql nvarchar(4000)
declare @LineEnd nvarchar(100)

--Drop _Fields View if it exists
exec('IF  EXISTS (SELECT * FROM sys.views WHERE object_id = OBJECT_ID(''' + @LinkedServer + '_Fields' + '''))
      DROP VIEW ' + @LinkedServer + '_Fields')

--Create _Fields View
set @sql = 'Create VIEW ' + @LinkedServer + '_Fields
As 
select QualifiedApiName as ObjectName, 
		Particles_QualifiedApiName as FieldName,
		Particles_DataType as Type,
		Particles_Label as Label,
		Particles_MasterLabel as MasterLabel,
		Particles_Length as Length,
		Particles_Precision as Precision,
		Particles_Scale as Scale,
		Particles_RelationshipName as RelationshipName,
		Particles_DeveloperName as DeveloperName,
		Particles_DurableId as DurableId,
		Particles_DefaultValueFormula as DefaultValueFormula,
		Particles_Digits as Digits,
		Particles_ExtraTypeInfo as ExtraTypeInfo,
		Particles_FieldDefinitionId as FieldDefinitionId,
		Particles_InlineHelpText as InlineHelpText,
		Particles_IsApiFilterable as IsApiFilterable,
		Particles_IsApiGroupable as IsApiGroupable,
		Particles_IsApiSortable as IsApiSortable,
		Particles_IsAutonumber as IsAutonumber,
		Particles_IsCalculated as IsCalculated,
		Particles_IsCaseSensitive as IsCaseSensitive,
		Particles_IsCompactLayoutable as IsCompactLayoutable,
		Particles_IsComponent as IsComponent,
		Particles_IsCompound as IsCompound,
		Particles_IsCreatable as IsCreatable,
		Particles_IsDefaultedOnCreate as IsDefaultedOnCreate,
		Particles_IsDependentPicklist as IsDependentPicklist,
		Particles_IsDeprecatedAndHidden as IsDeprecatedAndHidden,
		Particles_IsDisplayLocationInDecimal as IsDisplayLocationInDecimal,
		Particles_IsEncrypted as IsEncrypted,
		Particles_IsFieldHistoryTracked as IsFieldHistoryTracked,
		Particles_IsHighScaleNumber as IsHighScaleNumber,
		Particles_IsHtmlFormatted as IsHtmlFormatted,
		Particles_IsIdLookup as IsIdLookup,
		Particles_IsLayoutable as IsLayoutable,
		Particles_IsListVisible as IsListVisible,
		Particles_IsNameField as IsNameField,
		Particles_IsNamePointing as IsNamePointing,
		Particles_IsNillable as IsNillable,
		Particles_IsPermissionable as IsPermissionable,
		Particles_IsUnique as IsUnique,
		Particles_IsUpdatable as IsUpdatable,
		Particles_IsWorkflowFilterable as IsWorkflowFilterable,
		Particles_IsWriteRequiresMasterRead as IsWriteRequiresMasterRead,
		Particles_Mask as Mask,
		Particles_MaskType as MaskType,
		Particles_Name as Name,
		Particles_NamespacePrefix as NamespacePrefix,
		Particles_ReferenceTargetField as ReferenceTargetField,
		Particles_RelationshipOrder as RelationshipOrder
		from openquery(' + @LinkedServer + ', ' + '''SELECT QualifiedApiName, 
		(Select QualifiedApiName, 
				DataType, 
				Label, 
				MasterLabel, 
				Length, 
				Precision, 
				Scale, 
				RelationshipName, 
				DeveloperName, 
				DurableId, 
				DefaultValueFormula, 
				Digits,
				ExtraTypeInfo, 
				FieldDefinitionId, 
				InlineHelpText, 
				IsApiFilterable, 
				IsApiGroupable, 
				IsApiSortable, 
				IsAutonumber, 
				IsCalculated, 
				IsCaseSensitive, 
				IsCompactLayoutable, 
				IsComponent, 
				IsCompound, 
				IsCreatable, 
				IsDefaultedOnCreate, 
				IsDependentPicklist, 
				IsDeprecatedAndHidden, 
				IsDisplayLocationInDecimal, 
				IsEncrypted, 
				IsFieldHistoryTracked, 
				IsHighScaleNumber, 
				IsHtmlFormatted, 
				IsIdLookup, 
				IsLayoutable, 
				IsListVisible, 
				IsNameField, 
				IsNamePointing, 
				IsNillable, 
				IsPermissionable,
				IsUnique, 
				IsUpdatable, 
				IsWorkflowFilterable, 
				IsWriteRequiresMasterRead, 
				Mask, 
				MaskType, 
				Name, 
				NamespacePrefix, 
				ReferenceTargetField, 
				RelationshipOrder
		 from Particles) 
		 From EntityDefinition'')'

exec sp_executesql @sql


--Drop _FieldsPerObject View if it exists
exec('IF  EXISTS (SELECT * FROM sys.views WHERE object_id = OBJECT_ID(''' + @LinkedServer + '_FieldsPerObject' + '''))
      DROP VIEW ' + @LinkedServer + '_FieldsPerObject')

--Create _FieldsPerObject View
set @sql = 'Create VIEW ' + @LinkedServer + '_FieldsPerObject
As 
select QualifiedApiName as FieldName,
	    DataType as Type,
	    Label,
		MasterLabel,
		Length,
		Precision,
		Scale,
		RelationshipName,
		DeveloperName,
		DurableId,
		DefaultValueFormula,
		Digits,
		ExtraTypeInfo,
		FieldDefinitionId,
		InlineHelpText,
		IsApiFilterable,
		IsApiGroupable,
		IsApiSortable,
		IsAutonumber,
		IsCalculated,
	    IsCaseSensitive,
		IsCompactLayoutable,
		IsComponent,
		IsCompound,
		IsCreatable,
		IsDefaultedOnCreate,
		IsDependentPicklist,
		IsDeprecatedAndHidden,
		IsDisplayLocationInDecimal,
		IsEncrypted,
		IsFieldHistoryTracked,
		IsHighScaleNumber,
		IsHtmlFormatted,
		IsIdLookup,
		IsLayoutable,
		IsListVisible,
		IsNameField,
	    IsNamePointing,
		IsNillable,
		IsPermissionable,
		IsUnique,
		IsUpdatable,
		IsWorkflowFilterable,
		IsWriteRequiresMasterRead,
		Mask,
		MaskType,
		Name,
		NamespacePrefix,
		ReferenceTargetField,
		RelationshipOrder,
		EntityDefinitionId as ObjectName
		from ' + @LinkedServer + '.CData.Salesforce.EntityParticle' 

exec sp_executesql @sql


--Drop _Objects View if it exists
exec('IF  EXISTS (SELECT * FROM sys.views WHERE object_id = OBJECT_ID(''' + @LinkedServer + '_Objects' + '''))
      DROP VIEW ' + @LinkedServer + '_Objects')

--Create _Objects View
set @sql = 'Create VIEW ' + @LinkedServer + '_Objects
As 
select QualifiedApiName as ObjectName,
	    DefaultCompactLayoutId,
	    DetailUrl,
		DeveloperName,
		DurableId,
		EditDefinitionUrl,
		EditUrl,
		ExternalSharingModel,
		HasSubtypes,
		HelpSettingPageName,
		HelpSettingPageUrl,
		InternalSharingModel,
		IsApexTriggerable,
		IsAutoActivityCaptureEnabled,
		IsCompactLayoutable,
		IsCustomizable,
		IsCustomSetting,
		IsDeprecatedAndHidden,
		IsEverCreatable,
		IsEverDeletable,
		IsEverUpdatable,
		IsFeedEnabled,
		IsIdEnabled,
		IsLayoutable,
		IsMruEnabled,
		IsProcessEnabled,
		IsQueryable,
		IsReplicateable,
		IsRetrieveable,
		IsSearchable,
		IsSearchLayoutable,
		IsSubtype,
		IsTriggerable,
		IsWorkflowEnabled,
		KeyPrefix,
		Label,
		LastModifiedById,
		LastModifiedDate,
		MasterLabel,
		NamespacePrefix,
		NewUrl,
		PluralLabel,
		PublisherId,
		RunningUserEntityAccessId
		from ' + @LinkedServer + '.CData.Salesforce.EntityDefinition' 

exec sp_executesql @sql


--Drop _Relationships View if it exists
exec('IF  EXISTS (SELECT * FROM sys.views WHERE object_id = OBJECT_ID(''' + @LinkedServer + '_Relationships' + '''))
      DROP VIEW ' + @LinkedServer + '_Relationships')

--Create _Relationships View
set @sql = 'Create VIEW ' + @LinkedServer + '_Relationships
As 
select RelationshipDomains_ParentSobjectId as ParentObject,
		RelationshipDomains_ChildsObject_QualifiedApiName as ChildObject,
		RelationshipDomains_RelationshipName as ParentToChildRelationshipName,
		RelationshipDomains_IsCascadeDelete as IsCascadeDelete,
		RelationshipDomains_IsRestrictedDelete as IsRestrictedDelete
from openquery(' + @LinkedServer + ', ' + '''Select QualifiedApiName, 
(Select ParentSobjectId, 
		ChildsObject.QualifiedApiName,
		RelationshipName,
		IsCascadeDelete,
		IsRestrictedDelete
from RelationshipDomains) 
from EntityDefinition'')' +
' where RelationshipDomains_RelationshipName is not null
and RelationshipDomains_ParentSobjectId not like ''01%''
and RelationshipDomains_ChildsObject_QualifiedApiName not like ''01%'''

exec sp_executesql @sql


--Drop _UserEntityAccess View if it exists
exec('IF  EXISTS (SELECT * FROM sys.views WHERE object_id = OBJECT_ID(''' + @LinkedServer + '_UserEntityAccess' + '''))
      DROP VIEW ' + @LinkedServer + '_UserEntityAccess')

--Create _UserEntityAccess View
set @sql = 'Create VIEW ' + @LinkedServer + '_UserEntityAccess
As 
select EntityDefinitionId as ObjectName,
		DurableId,
		UserId,
		IsActivateable,
		IsCreatable,
		IsDeletable,
		IsEditable,
		IsFlsUpdatable,
		IsMergeable,
		IsReadable,
		IsUndeletable,
		IsUpdatable
		from ' + @LinkedServer + '.CData.Salesforce.UserEntityAccess' 

exec sp_executesql @sql

GO

