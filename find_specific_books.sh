#!/bin/bash

# Updated list of public domain children's books
book_list=(
[
    "Alice's Adventures in Wonderland",
    "Through the Looking-Glass",
    "The Adventures of Tom Sawyer",
    "Black Beauty",
    "The Merry Adventures of Robin Hood",
    "Heidi",
    "Little Lord Fauntleroy",
    "The Jungle Book",
    "The Second Jungle Book",
    "The Wonderful Wizard of Oz",
    "The Tale of Peter Rabbit",
    "Five Children and It",
    "Just So Stories",
    "The Call of the Wild",
    "A Little Princess",
    "The Railway Children",
    "Anne of Green Gables",
    "Peter Pan in Kensington Gardens",
    "The Wind in the Willows",
    "The Secret Garden",
    "Peter and Wendy",
    "The Velveteen Rabbit",
    "Winnie-the-Pooh",
    "Now We Are Six",
    "The House at Pooh Corner",
    "The Adventures of Pinocchio",
    "Grimm's Fairy Tales",
    "Andersen's Fairy Tales",
    "The Swiss Family Robinson",
    "A Child's Garden of Verses",
    "The Princess and the Goblin",
    "At the Back of the North Wind",
    "What Katy Did",
    "Little Women",
    "Hans Brinker or the Silver Skates",
    "The Water-Babies",
    "The Coral Island",
    "Tom Brown's School Days",
    "The King of the Golden River",
    "Slovenly Peter",
    "Tales from Shakespeare",
    "The Blue Fairy Book",
    "The Red Fairy Book",
    "The Green Fairy Book",
    "The Yellow Fairy Book",
    "The Pink Fairy Book",
    "The Grey Fairy Book",
    "The Violet Fairy Book",
    "The Crimson Fairy Book",
    "The Brown Fairy Book",
    "The Orange Fairy Book",
    "The Olive Fairy Book",
    "The Lilac Fairy Book",
    "Prince Prigio",
    "The Story of the Treasure Seekers",
    "The Wouldbegoods",
    "The Phoenix and the Carpet",
    "The Story of Doctor Dolittle",
    "The Voyages of Doctor Dolittle",
    "Mother Goose in Prose",
    "The Adventures of Uncle Remus",
    "Nights with Uncle Remus",
    "The Princess and Curdie",
    "Ranald Bannerman's Boyhood",
    "What Katy Did at School",
    "What Katy Did Next",
    "Eight Cousins",
    "Rose in Bloom",
    "Under the Lilacs",
    "Jack and Jill",
    "Jo's Boys",
    "Little Men",
    "The Bobbsey Twins",
    "The Scarecrow of Oz",
    "Rinkitink in Oz",
    "The Lost Princess of Oz",
    "The Tin Woodman of Oz",
    "Ozma of Oz",
    "Dorothy and the Wizard in Oz",
    "The Patchwork Girl of Oz",
    "Tik-Tok of Oz",
    "The Magic of Oz",
    "Glinda of Oz",
    "The Road to Oz",
    "The Emerald City of Oz",
    "Queen Silver-Bell",
    "Sara Crewe: or, What Happened at Miss Minchin's",
    "The Secret of the Old Mill",
    "The Happy Prince and Other Tales",
    "A Chinese Wonder Book",
    "The Story of a China Cat",
    "Donegal Fairy Tales",
    "Celtic Fairy Tales",
    "More English Fairy Tales",
    "English Fairy Tales",
    "A Book of Nonsense",
    "The Railway Adventures of Gerard",
    "The Burgess Bird Book for Children"
])

# Check if calibredb is installed
if ! command -v calibredb &> /dev/null
then
    echo "Error: calibredb command not found. Please make sure Calibre is installed and the command is in your system's PATH."
    exit 1
fi

echo "Checking for the following books in your Calibre library (batch search with publication dates):"
echo "--------------------------------------------------------------------------------------"

# Get all titles and publication dates from the Calibre library
calibre_data=$(calibredb list --fields title,pubdate)

# Create an associative array to store titles and their publication dates
declare -A calibre_info

# Process the output of calibredb list
while IFS=$'\t' read -r title pubdate; do
    calibre_info["$title"]="$pubdate"
done <<< "$calibre_data"

found_count=0
found_books=""
not_found_books=""

for book_title in "${book_list[@]}"; do
    # Use grep -iF for case-insensitive fixed-string search
    if echo "$calibre_data" | grep -qiF "$book_title"; then
        pub_date="${calibre_info["$book_title"]}"
        echo "Found: ${book_title} (Publication Date: ${pub_date})"
        found_count=$((found_count + 1))
        found_books+="- ${book_title} (Publication Date: ${pub_date})\n"
    else
        echo "Not found: ${book_title}"
        not_found_books+="- ${book_title}\n"
    fi
done

echo "--------------------------------------------------------------------------------------"
echo "Found ${found_count} out of ${#book_list[@]} books in your Calibre library."

if [[ -n "$found_books" ]]; then
    echo ""
    echo "List of found books with publication dates:"
    echo "$found_books"
fi

if [[ -n "$not_found_books" ]]; then
    echo ""
    echo "List of books not found:"
    echo "$not_found_books"
fi
