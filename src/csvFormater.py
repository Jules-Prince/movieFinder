import csv

def format_movie_name(movie_name):
    movie_name = movie_name.replace(':', '')
    return movie_name.strip('"').replace(',', ' ')

def main(input_file, output_file):
    with open(input_file, 'r', encoding='utf-8') as infile, \
         open(output_file, 'w', encoding='utf-8', newline='') as outfile:
        
        reader = csv.reader(infile)
        writer = csv.writer(outfile)

        header = next(reader)
        writer.writerow(header)

        for row in reader:
            movie_id, movie_name, genres = row[0], row[1], row[2]

            formatted_movie_name = format_movie_name(movie_name)

            writer.writerow([movie_id, formatted_movie_name, genres])

if __name__ == "__main__":
    input_csv = "data/ml-25m/movies.csv"
    output_csv = "output.csv"
    main(input_csv, output_csv)