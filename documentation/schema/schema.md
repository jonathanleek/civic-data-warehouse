Overview
========
While the initial scope of this project is Saint Louis City, it is intended to be globally scalable from a data model perspective. To this end, the schema is being designed very generically, and is focusing on data that is commonly available and follows common patterns across regions. For example, pretty much every city has building permits of various kinds, which contain mostly the same sorts of information, so they are included. Tax incentive programs, however, can vary wildly from region to region, so only metadata regarding participation will be included.

The Spine
=========
The CDW data model is primarily a hub and spoke schema, with a hierarchical hub 'spine'. This spine represents the core objects that CDW will be tracking; properties. Each table of the spine can be thought of as a further geographic subsectioning of the table above it. The base unit of the CDW is the Parcel; a plot of line defined by local municipal body, which is represented in the parcel table. A parcel may contain one or more buildings, which are represented in the Building table. A building might contain multiple units (condos, apartments, etc), which are tracked in the Units table.

Data is tied to the lowest level of the spine that is applicable. For example, an apartment building contains many addresses, so address ties to the Unit, not building. Because of this, each building must have at least one unit. In the case of a single unit building, the Unit table will contain the pertinent information for the entire building.

Outlies and Special Cases
-------------------------
In some cities, there are non-real-estate taxable properties that are assessed and mixed in with more traditional parcels like lots and buildings. In order to accurately represent these