In deze UseCase zijn de enkelvoudige sleutels vervangen door dubbele. Aan elke
entiteit is een attribuut met de code Source toegevoegd. Dit attribuut is een
deel van de primarykey.
Hierdoor is er ook een verandering opgetreden in de identifiers. 
De eerste identifier maakt gebruik van de primarykey en is door toevoegen van
Source veranderd. Ik heb de naak aangepast en er PK achter geplakt om
duidelijk onderscheid te kunnen maken.
Ik heb nieuwe identifiers aangemaakt voor de relaties met entiteiten die
alleen gebruik maken van de TID's.

voorbeeld
Oude PK  DeclarationTID  (kan gebruikt worden voor bv relatie met Consshipment)

Nieuwe PK 'DMS'+DeclarationTID (kan niet meer worden gebruikt voor de relatie
met Consshipment) 

Nieuwe identifier met alleen de DeclarationTID als attribuut kan weer worden 
gebruikt voor  de relatie met Consshipment.