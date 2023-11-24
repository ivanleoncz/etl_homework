import csv
import logging

logging.basicConfig(format='%(asctime)s,%(levelname)s,%(message)s', filename='csv_processing.log',
                    level=logging.DEBUG, datefmt='%Y-%m-%d %H:%M:%S')


class CSVConverter:

    def __init__(self, input_csv, output_csv, format_example_csv):
        self.header_mapping = None
        self.input_csv = input_csv
        self.output_csv = output_csv
        self.format_example_csv = format_example_csv

    @staticmethod
    def convert_column_upc(upc) -> str:
        """
        Converts UPC data into EAN13.
        """
        return "0" + str(upc[:2]) + "-" + str(upc[2:11]) + "-" + str(upc[-1]) if upc else ""

    def convert(self):
        items_processed = 0
        logging.info("process started!")
        with open(self.input_csv, "r") as input_file, open(self.format_example_csv, "r") as format_example_csv, \
                open(self.output_csv, "w") as output_csv:

            logging.info("reading CSV files")
            input_reader = csv.DictReader(input_file, delimiter=',')
            example_reader = csv.DictReader(format_example_csv, delimiter=',')

            logging.info(f"starting {self.input_csv} processing + writing results into {self.output_csv}")
            writer = csv.DictWriter(output_csv, fieldnames=example_reader.fieldnames)
            writer.writeheader()
            for row in input_reader:
                logging.info(f"processing item number {row['item number']}")
                try:
                    writer.writerow({
                        "ean13": self.convert_column_upc(row["upc"]),
                        "weight": row["item weight (pounds)"],
                        "length": row["item depth (inches)"],
                        "width": row["item width (inches)"],
                        "height": row["item height (inches)"],
                        "prop_65": (True if "P65" in row["url california label (jpg)"] or
                                            "P65" in row["url california label (pdf)"] else False),
                        "cost_price": row["wholesale ($)"],
                        "min_price": row["map ($)"],
                        "made_to_order": None,
                        "product__product_class__name": row["item category"],
                        "product__brand__name": row["brand"],
                        "product__title": row["item type"],
                        "product__description": row["description"],
                        "product__bullets__0": row["selling point 1"],
                        "product__bullets__1": row["selling point 2"],
                        "product__bullets__2": row["selling point 3"],
                        "product__bullets__3": row["selling point 4"],
                        "product__bullets__4": row["selling point 5"],
                        "product__bullets__5": row["selling point 6"],
                        "product__bullets__6": row["selling point 7"],
                        "product__configuration__codes": None,
                        "product__multipack_quantity": None,
                        "product__country_of_origin__alpha_3": row["country of origin"][:3],
                        "product__parent_sku": None,
                        "attrib__arm_height": row["furniture arm height (inches)"],
                        "attrib__assembly_required": None,
                        "attrib__back_material": None,
                        "attrib__blade_finish": None,
                        "attrib__bulb_included": True if row["bulb 1 included"] else False,
                        "attrib__bulb_type": row["bulb 1 type"],
                        "attrib__color": row["primary color family"],
                        "attrib__cord_length": row["cord length (inches)"],
                        "attrib__design_id": None,
                        "attrib__designer": None,
                        "attrib__distressed_finish": None,
                        "attrib__fill": None,
                        "attrib__finish": row["item finish"],
                        "attrib__frame_color": None,
                        "attrib__hardwire": None,
                        "attrib__kit": row["conversion kit option"] if row["conversion kit option"] else None,
                        "attrib__leg_color": None,
                        "attrib__leg_finish": None,
                        "attrib__material": row["item materials"],
                        "attrib__number_bulbs": (int(row["bulb 1 count"]) + int(row["bulb 2 count"])
                                                 if row["bulb 1 count"].isdigit()
                                                 and row["bulb 2 count"].isdigit()
                                                 else row["bulb 1 count"]
                                                 or row["bulb 2 count"]),
                        "attrib__orientation": None,
                        "attrib__outdoor_safe": row["outdoor"],
                        "attrib__pile_height": None,
                        "attrib__seat_depth": None,
                        "attrib__seat_height": row["furniture seat height (inches)"],
                        "attrib__seat_width": row["furniture seat dimensions (inches)"],
                        "attrib__shade": True if row["shade/glass description"] else False,
                        "attrib__size": None,
                        "attrib__switch_type": row["switch type"],
                        "attrib__ul_certified": None,
                        "attrib__warranty_years": None,
                        "attrib__wattage": row["bulb 1 wattage"],
                        "attrib__weave": None,
                        "attrib__weight_capacity": row["furniture weight capacity (pounds)"],
                        "boxes__0__weight": row["carton 1 weight (pounds)"],
                        "boxes__0__length": row["carton 1 length (inches)"],
                        "boxes__0__height": row["carton 1 height (inches)"],
                        "boxes__0__width": row["carton 1 width (inches)"],
                        "boxes__1__weight": row["carton 2 weight (pounds)"],
                        "boxes__1__length": row["carton 2 length (inches)"],
                        "boxes__1__height": row["carton 2 height (inches)"],
                        "boxes__1__width": row["carton 2 width (inches)"],
                        "boxes__2__weight": row["carton 3 weight (pounds)"],
                        "boxes__2__length": row["carton 3 length (inches)"],
                        "boxes__2__height": row["carton 3 height (inches)"],
                        "boxes__2__width": row["carton 3 width (inches)"],
                        "boxes__3__weight": None,
                        "boxes__3__length": None,
                        "boxes__3__height": None,
                        "boxes__3__width": None,
                        "product__styles": ", ".join((row["item style"], row["item substyle"],
                                                      row["item substyle 2"])).strip(', ')
                    })
                    items_processed += 1
                except Exception as e:
                    logging.error(f"exception while processing {row['item number']}: ", e)
            logging.info(f"amount of items processed: {items_processed}")
            logging.info("process finished!")


if __name__ == "__main__":
    csv_converter = CSVConverter(input_csv="homework.csv", output_csv="formatted.csv", format_example_csv="example.csv")
    csv_converter.convert()
