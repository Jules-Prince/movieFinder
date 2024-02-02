import csv

def formatMovieName(movieName):
    movieName = movieName.replace(':', '')
    return movieName.strip('"').replace(',', ' ')

def main(inputFile, outputFile):
    with open(inputFile, 'r', encoding='utf-8') as infile, \
         open(outputFile, 'w', encoding='utf-8', newline='') as outfile:
        
        reader = csv.reader(infile)
        writer = csv.writer(outfile)

        header = next(reader)
        writer.writerow(header)

        for row in reader:
            movieId, movieName, genres = row[0], row[1], row[2]

            formattedMovieName = formatMovieName(movieName)

            writer.writerow([movieId, formattedMovieName, genres])

if __name__ == "__main__":
    inputCSV = "data/ml-25m/movies.csv"
    outputCSV = "output.csv"
    main(inputCSV, outputCSV)
