Overview
========
While the initial scope of this project is Saint Louis City, it is intended to be globally scalable from a data model perspective. To this end, the schema is being designed very generically, and is focusing on data that is commonly available and follows common patterns across regions. For example, pretty much every city has building permits of various kinds, which contain mostly the same sorts of information, so they are included. Tax incentive programs, however, can vary wildly from region to region, so only metadata regarding participation will be included.

The Spine
=========
The CDW data model is primarily a hub and spoke schema, with a hierarchical hub 'spine'. This spine represents the core objects that CDW will be tracking; properties. Each table of the spine can be thought of as a further geographic subsectioning of the table above it. The base unit of the CDW is the Parcel; a plot of line defined by local municipal body, which is represented in the parcel table. A parcel may contain one or more buildings, which are represented in the Building table. A building might contain multiple units (condos, apartments, etc), which are tracked in the Units table.

Data is tied to the lowest level of the spine that is applicable. For example, an apartment building contains many addresses, so address ties to the Unit, not building. Because of this, each building must have at least one unit. In the case of a single unit building, the Unit table will contain the pertinent information for the entire building.

Parcel vs. assessment account
-----------------------------
A city's assessment file is keyed by *taxable account*, not strictly by *plot of land*. In St.
Louis the assessor `handle` identifies the **plot of land (GIS footprint)**, while multiple
*accounts* can share one `handle` — most commonly the individual units of a condominium, which sit
on a single shared footprint. CDW resolves this to the spine as follows:

- The plot of land (`handle`) becomes a **parcel**.
- Each account sharing that footprint (e.g. each condo unit) becomes a **unit** beneath it.

This is the intended mapping: the assessor's flat file conflates land and units, and the CDW spine
separates them.

Outliers and Special Cases — real-estate entities only
------------------------------------------------------
The CDW spine holds **real-estate entities only** (land, buildings, units). Some jurisdictions
assess non-real-estate taxable items as "parcels" — e.g. billboards / outdoor advertising
structures and other non-real-estate possessory interests. These are **filtered out** of CDW.

Filtering source records is a deliberate, sanctioned **exception** to CDW's general rule of
restructuring the city's data without altering it; it applies only to non-real-estate items, which
fall outside the data model's scope. Excluded records are written to an exclusion log for
transparency rather than silently dropped. (Note: parking lots/garages and condominium garage
spaces are real estate and are retained.)