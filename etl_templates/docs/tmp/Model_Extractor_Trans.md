classDiagram
    class Mapping {
        +String Id
        +String ObjectID
        +String Name
        +String Code
        +String CreationDate
        +String Creator
        +String ModificationDate
        +String Modifier
    }
    class EntityTarget {
        +String Id
        +String Name
        +String Code
        +String IdModel
        +String NameModel
        +String CodeModel
        +Boolean IsDocumentModel
    }
        class SourceObject {
        +Integer Order
        +String Id
        +String EntityAlias
        +String ObjectID
        +String Name
        +String Code
        +String JoinType
        +String JoinAlias
    }
    class AttributeMapping {
        +String Id
        +String ObjectID
        +Integer Order
    }
    class JoinCondition {
        +Integer Order
        +String Id
        +String ObjectID
        +String Name
        +String Code
        +String JoinOperator
    }
    class ConditionComponents{
        +String LiteralValue
    }
    class ComponentAttribute{
        + String Id
        + String Name
        + String Code
        + String IdModel
        + String NameModel
        + String CodeModel
        + Boolean IsDocumentModel
        + String IdEntity
        + String NameEntity
        + String CodeEntity
        + String EntityAlias
    }
    
    Mapping "1" *-- "1" EntityTarget
    Mapping "1" *-- "*" SourceObject
    Mapping "1" *-- "*" AttributeMapping
    SourceObject "1" *-- "*" JoinCondition
    JoinCondition "1" *-- "*" ConditionComponents
    ConditionComponents "1" *-- "*" AttributeChild
    ConditionComponents "1" *-- "*" AttributeParent
    ComponentAttribute <|-- AttributeChild
    ComponentAttribute <|-- AttributeParent

    note for ComponentAttribute "Is either AttributeChild or AttributeParent"