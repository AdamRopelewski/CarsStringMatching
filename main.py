import csv
import io
import Levenshtein
import re
from time import time
import datetime
import threading
from concurrent.futures import ThreadPoolExecutor


def process_range(startIndex, endIndex):
    ListOfMatchesPerCar = calculateStringRatio(
        CarDataBaseDic, UsersInputFromDB, startIndex, endIndex
    )
    ListOfTopMatchesPerCar = getTopMatches(ListOfMatchesPerCar, 2, startIndex, endIndex)
    writeMatchesToCSV(
        ListOfTopMatchesPerCar, f"ListOfTopMatchesPerCar_{startIndex}_{endIndex}.csv"
    )
    print(
        f"\nWrote matches top per car into csv file from {startIndex} to {endIndex}\n"
    )


def readCsvFromFile(file_path: str) -> dict:
    """
    Reads data from a CSV file and converts it into a dictionary.

    Args:
        file_path (str): The path to the CSV file.

    Returns:
        dict: A dictionary where keys are column headers and values are lists of values under each header.

    Example:
        Given a CSV file 'data.csv' with contents:
        ```
        Name; Age; City
        John; 30; New York
        Alice; 25; Los Angeles
        ```
        The function call `readCsvFromFile('data.csv')` would return:
        ```
        {
            'Name': ['John', 'Alice'],
            'Age': ['30', '25'],
            'City': ['New York', 'Los Angeles']
        }
    """
    data = {}
    delimiter = "; "
    with open(file_path, mode="r") as file:
        # Read the file content and replace the multi-character delimiter with a single character
        content = file.read().replace(delimiter, ";")
        content2 = content.replace("PlaceholderGen", "")
        # Use StringIO to create a file-like object from the modified content
        file_like_object = io.StringIO(content2)
    reader = csv.DictReader(file_like_object, delimiter=";")
    for row in reader:
        for key, value in row.items():
            # Assuming the first column contains unique keys
            if key not in data:
                data[key] = []
            data[key].append(value)
    return data


def calculateStringRatio(CarDataBaseDic, UsersInputFromDB, startIndex, endIndex):
    """
    Calculates string similarity ratios between user input and car database entries.

    Args:
        CarDataBaseDic (dict): A dictionary containing car database information with keys:
                               'Brand', 'Model', 'Generation', 'Version'.
        UsersInputFromDB (dict): A dictionary containing user input information with keys:
                                  'car_id', 'marka', 'model'.
        startIndex (int): The start index for iterating over car IDs.
        endIndex (int): The end index for iterating over car IDs.

    Returns:
        list: A list of dictionaries, each containing a list of matches for a car with keys:
              'CarId' (str): The ID of the car.
              'ListOfMatches' (list): A list of dictionaries, each containing match details with keys:
                                       'Ratio' (float): The similarity ratio between user input and car.
                                       'MatchedID' (int): The ID of the matched car in the database.
                                       'MatchedCar' (dict): A dictionary containing matched car details with keys:
                                                            'Brand' (str): The brand of the matched car.
                                                            'Model' (str): The model of the matched car.
                                                            'Generation' (str): The generation of the matched car.
                                                            'Version' (str): The version of the matched car.
    """
    ListOfMatchesPerCar = []

    def calculateRatioBasedOnYear(CarIdFromDB, CarId, ratio):
        current_year = datetime.datetime.now().year
        yearsSearchQuerryFromCarDataBase = (
            CarDataBaseDic["Generation"][CarIdFromDB]
            + " "
            + CarDataBaseDic["Version"][CarIdFromDB]
        )
        try:
            rok = int(UsersInputFromDB["rok"][CarId])
        except ValueError:
            rok = 10000
        if rok == 0:
            rok = 10000
        matches = re.findall(r"\b(\d{4}-\d{4})\b", yearsSearchQuerryFromCarDataBase)
        matchesYearsBeyond = re.findall(
            r"\b(\d{4}-teraz)\b", yearsSearchQuerryFromCarDataBase
        )
        if len(matches) == 0:
            matches = matchesYearsBeyond
        year1 = year2 = year3 = year4 = -1

        if len(matches) == 2:
            year1 = int(matches[0][:4])
            year2 = matches[0][5:9]
            if year2 == "tera":
                year2 = current_year
            else:
                year2 = int(year2)
            year3 = int(matches[1][:4])
            year4 = matches[1][5:9]
            if year4 == "tera":
                year4 = current_year
            else:
                year4 = int(year2)
        elif len(matches) == 1:
            year1 = int(matches[0][:4])
            year2 = matches[0][5:9]
            if year2 == "tera":
                year2 = current_year
            else:
                year2 = int(year2)

        if year2 - rok >= 0 and year1 - rok <= 0:
            ratio += 0.2
        if year4 - rok >= 0 and year3 - rok <= 0:
            ratio += 0.1
        return ratio

    for CarId in range(startIndex, endIndex):
        MachesPerCarDict = {"CarId": "", "ListOfMatches": []}
        MachesPerCarDict["CarId"] = UsersInputFromDB["car_id"][CarId]
        for CarIdFromDB in range(len(CarDataBaseDic["Brand"])):
            searchQuerryFromUsersInput = (
                UsersInputFromDB["marka"][CarId].capitalize()
                + " "
                + UsersInputFromDB["model"][CarId].capitalize()
            )
            searchQuerryFromCarDataBase1 = (
                CarDataBaseDic["Brand"][CarIdFromDB].capitalize()
                + " "
                + CarDataBaseDic["Model"][CarIdFromDB]
            )
            input_string = (
                CarDataBaseDic["Generation"][CarIdFromDB]
                + " "
                + CarDataBaseDic["Version"][CarIdFromDB]
            )
            searchQuerryFromCarDataBase2 = re.sub(r"\(\d{4}-\d{4}\)", "", input_string)
            searchQuerryFromCarDataBase2 = re.sub(
                r"\(\d{4}-teraz\)", "", searchQuerryFromCarDataBase2
            )
            searchQuerryFromCarDataBase = (
                searchQuerryFromCarDataBase1 + " " + searchQuerryFromCarDataBase2
            )
            ratio = Levenshtein.ratio(
                searchQuerryFromUsersInput,
                searchQuerryFromCarDataBase,
            )
            ratio = calculateRatioBasedOnYear(CarIdFromDB, CarId, ratio)
            ratio /= 1.3
            ratio = round(ratio, 5)

            MatchedCarDict = {
                "Brand": CarDataBaseDic["Brand"][CarIdFromDB],
                "Model": CarDataBaseDic["Model"][CarIdFromDB],
                "Generation": CarDataBaseDic["Generation"][CarIdFromDB],
                "Version": CarDataBaseDic["Version"][CarIdFromDB],
            }
            aMatchDict = {
                "Ratio": ratio,
                "MatchedID": CarIdFromDB,
                "MatchedCar": MatchedCarDict,
            }
            MachesPerCarDict["ListOfMatches"].append(aMatchDict)

        ListOfMatchesPerCar.append(MachesPerCarDict)
        print(f"Done string comparison: {CarId}/{endIndex}")
    return ListOfMatchesPerCar


def getTopMatches(
    ListOfMatchesPerCar: list, amount: int, startIndex: int, endIndex: int
):
    """
    Returns a list of top matches for each car in ListOfMatchesPerCar.

    Args:
        ListOfMatchesPerCar (list): A list containing dictionaries for each car, where each dictionary
                                    contains a key 'ListOfMatches' which is a list of dictionaries
                                    representing matches for that car.
        amount (int): The number of top matches to retrieve for each car.

    Returns:
        list: A list of dictionaries, each containing the top 'amount' matches for a car.
    """
    # ListOfTopMatchesPerCar = copy.deepcopy(ListOfMatchesPerCar)
    n = len(ListOfMatchesPerCar)
    # i = 0
    for matchDict in ListOfMatchesPerCar:
        matches = matchDict["ListOfMatches"]
        topMatches = sorted(matches, key=lambda x: x["Ratio"], reverse=True)[:amount]
        matchDict["ListOfMatches"] = topMatches
        print(f"Done getting top matches: {ListOfMatchesPerCar.index(matchDict)}/{n}")
    return ListOfMatchesPerCar


def writeMatchesToCSV(ListOfMatchesPerCar: list, path: str) -> None:
    """
    Writes matches per car to a CSV file.

    Args:
        ListOfMatchesPerCar (list): A list containing dictionaries for each car, where each dictionary
                                    contains a key 'CarId' and a list of matches with keys 'Ratio',
                                    'MatchedID', and 'MatchedCar'.
        path (str): The file path where the CSV file will be created.

    Returns:
        None
    """
    output = [
        "CarID;Ratio;MatchedId;MatchedBrand;MatchedModel;MatchedGeneration;MatchedVersion"
    ]
    for car in ListOfMatchesPerCar:
        for k in range(len(car["ListOfMatches"])):
            line = f"{car['CarId']};{car['ListOfMatches'][k]['Ratio']}"
            matched_car = car["ListOfMatches"][k]["MatchedCar"]
            line = (
                f"{car['CarId']};"
                f"{car['ListOfMatches'][k]['Ratio']};"
                f"{car['ListOfMatches'][k]['MatchedID']};"
                f"{matched_car['Brand']};"
                f"{matched_car['Model']};"
                f"{matched_car['Generation']};"
                f"{matched_car['Version']}"
            )
            output.append(line)
    try:
        f = open(path, "w")
        f.write("\n".join(output))
    finally:
        f.close()


def writeCombinedMatchesToCSV(path: str, divideTo) -> None:
    """
    Writes combined matches to a CSV file.

    Args:
        path (str): The file path where the CSV file will be written.
        divideTo (int): The number used for dividing the data into multiple files.

    Returns:
        None
    """
    output = [
        "CarId;Ratio;MatchedId;MatchedBrand;MatchedModel;MatchedGeneration;MatchedVersion"
    ]
    for i in range(n // divideTo + 1):
        startIndex = divideTo * i
        endIndex = divideTo * (i + 1)
        if endIndex > n:
            endIndex = n
        read = readCsvFromFile(f"ListOfTopMatchesPerCar_{startIndex}_{endIndex}.csv")
        for i in range(len(read["CarID"])):
            lineToWrite = f"{read['CarID']};{read['Ratio']}"
            lineToWrite = (
                f"{read['CarID'][i]};"
                f"{read['Ratio'][i]};"
                f"{read['MatchedId'][i]};"
                f"{read['MatchedBrand'][i]};"
                f"{read['MatchedModel'][i]};"
                f"{read['MatchedGeneration'][i]};"
                f"{read['MatchedVersion'][i]}"
            )
            output.append(lineToWrite)
    try:
        f = open(path, "w")
        f.write("\n".join(output))
    finally:
        f.close()


start = time()
CarDataBaseDic = readCsvFromFile("ListOfCarBrands.csv")
UsersInputFromDB = readCsvFromFile("solidDB.csv")

divideTo = 500
n = len(UsersInputFromDB["car_id"])

# Define the maximum number of threads
max_threads = 4  # Adjust this number according to your system's capabilities

# Create a ThreadPoolExecutor with a fixed number of threads
with ThreadPoolExecutor(max_workers=max_threads) as executor:
    # Iterate over the ranges and submit tasks to the executor
    for i in range(n // divideTo + 1):
        startIndex = divideTo * i
        endIndex = divideTo * (i + 1)
        if endIndex > n:
            endIndex = n
            executor.submit(process_range, startIndex, endIndex)


writeCombinedMatchesToCSV("ListOfALLTopMatchesPerCar_ALL.csv", divideTo)

print("\nWrote all TOP matches per car into csv file\n")

print(time() - start)
print()
