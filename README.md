# A Look at Wildlife Diseases in Utah

This project processes and analyses data on wildlife diseases using actual capture data from the State of Utah's Division of Wildlife Resources veterinarian office. This data is exceptionally expensive to collect, and I am deeply grateful to the Utah DWR for sharing such interesting real-world data with me for this project. 

This project also serves as the Capstone for the Udacity Data Scientist Nanodegree, and is graded according to [this rubric](https://learn.udacity.com/nanodegrees/nd025/parts/cd1971/lessons/c20e1b63-c711-475b-b1ba-3ea987081193/concepts/c20e1b63-c711-475b-b1ba-3ea987081193-project-rubric). 

## Project Definition

This project has the following parts:
1. An ETL pipeline (INSERT FILE NAME) which processes data from blood test lab results. The pipeline has two outputs:
    - A set of species-specific excel files matching the format used by the Utah DWR vet office. 
    - A SQLite database for further analysis within this project. 
2. A Jupyter notebook (INSERT FILE NAME) analysing the data to gain insights, and consists of the following sections:
    - Problem statement
    - Analysis
    - Methodology
    - Results
    - Conclusion
3. A Medium blog post written for a technical audience. 
4. The corresponding [github repo](https://github.com/epistemetrica/utah_dwr_python). 

## Motivation

As an avid outdoorsman, hiker, mountaineer, and big-game hunter, I have long had a passion for wildlife and wild places. And as it just so happens, a few years ago the State of Utah hired one of my oldest friends to be their wildlife veterinarian. 

I was intent on using the Udacity Data Scientist Capstone project to provide actual value, so I reached out to my friend to see if there was some data cleaning and analysis that I could do for her. This project is the result. 

## Data

The data come to me in two parts:
1. Excel sheets provided by the DWR Vet Office showing animal id, species, capture dates, and locations. 
2. PDFs from various labs showing the results of various disease tests performed on each animal. Tables were extracted from these PDFs into excel documents using [Tabula](https://tabula.technology/)'s desktop app. (A more talented data scientist perhaps could have figued out a way to do this using the tabula-py library, but I could not.)

The data is available to Udacity reviewers and mentors on request, but will not be made public due to the extremely high cost of gathering the data. 

## Libraries 

- pandas
- numpy
- re

## Instructions

## Acknowledgements

First and foremost, I thank Dr. Virginia Stout, DVM, at the Utah Department of Wildlife Resources for sharing this data with me and providing valuable insights into the nature of wildlife diseases. 

Specific references and acknowledgements are included throughout the notebook and blog post. 

I would also like to acknoledge:
- the amazing communities on Medium and Stack Exchange, without whom I would be totally useless as a data scientist!
- the Udacity Mentors and knowledge base for their quick responses and quality advice. 

## License

MIT License

Copyright (c) 2023 Adam Paul Wilson

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.