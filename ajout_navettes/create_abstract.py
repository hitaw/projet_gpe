import xml.etree.ElementTree as ET
from xml.dom import minidom

def generate_access_xml(
        transit_stop_ids,
        radius,
        average_speed,
        using_routed_distance,
        access_type,
        frequency,
        output_path):

    root = ET.Element("abstractAccessItems")

    for stop_id in transit_stop_ids:
        item = ET.SubElement(root, "abstractAccessItem")
        item_id = f"{stop_id}-0"

        item.set("id", item_id)
        item.set("transitStopId", stop_id)
        item.set("radius", str(radius))
        item.set("averageSpeed", str(average_speed))
        item.set("usingRoutedDistance", str(using_routed_distance).lower())
        item.set("accessType", access_type)
        item.set("frequency", str(frequency))

    rough_string = ET.tostring(root, 'utf-8')
    reparsed = minidom.parseString(rough_string)
    pretty_xml = reparsed.toprettyxml(indent="    ")

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(pretty_xml)


transit_stop_ids = {
    "IDFM:monomodalStopPlace:59354.link:pt_IDFM:monomodalStopPlace:59354" : "Antonypole - Wissous Centre",
    "IDFM:monomodalStopPlace:59355.link:pt_IDFM:monomodalStopPlace:59355" : "Massy Opéra",
    "IDFM:monomodalStopPlace:59357.link:pt_IDFM:monomodalStopPlace:59357" : "Marguerite Perey",
    "IDFM:monomodalStopPlace:59358.link:pt_IDFM:monomodalStopPlace:59358" : "Moulon Campus",
    "IDFM:monomodalStopPlace:59359.link:pt_IDFM:monomodalStopPlace:59359" : "Christ de Saclay",
    "IDFM:monomodalStopPlace:59360.link:pt_IDFM:monomodalStopPlace:59360" : "Guyancourt",
    "IDFM:monomodalStopPlace:59361.link:pt_IDFM:monomodalStopPlace:59361" : "Satory",
    "IDFM:monomodalStopPlace:59347.link:pt_IDFM:monomodalStopPlace:59347" : "Le Bourget - Aéroport",
    "IDFM:monomodalStopPlace:59348.link:pt_IDFM:monomodalStopPlace:59348" : "Gonesse",
    "IDFM:monomodalStopPlace:59352.link:pt_IDFM:monomodalStopPlace:59352" : "Le Mesnil-Amelot",
    "IDFM:monomodalStopPlace:59341.link:pt_IDFM:monomodalStopPlace:59341" : "Parc du Blanc-Mesnil",
    "IDFM:monomodalStopPlace:59342.link:pt_IDFM:monomodalStopPlace:59342" : "Aulnay - Val Francilia",
}

generate_access_xml(
    transit_stop_ids=transit_stop_ids.keys(),
    radius=10000,
    average_speed=60,
    using_routed_distance=True,
    access_type="feeder",
    frequency=600,
    output_path="feeder.xml"
)
    