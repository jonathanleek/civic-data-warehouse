Overview
========
The data schema is the most scalable part of this project, and the intention is for similar data warehouses to be able to be developed independently, while maintaining interoperability. To that end, the primary unique key needs to fulfill several distinct criteria:
1. Scale to every parcel on earth
2. Convey meaningful information about the geographical location of the entity being identified
3. Describe the hierarchical position of the entity being described (municipality, parcel, building, unit, etc)

The CDW_ID is being designed to fulfill these criteria, and will serve as the primary key for the CDW. It consists of a series of decimal seperated integers, each representing a geographic level of detail. Any level of detail not appropriate for a given level can be either dropped or zero'd out, and it will still be able to easily join to other tables and identify what the record corresponds to.

CCCC.cccccc.pppppppp.bbbb.uuuuu

CCCC - Country
cccccc - County
pppppppp - Parcel
bbbb - Building
uuuuu - Unit