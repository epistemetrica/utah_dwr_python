# A Look at Wildlife Diseases in Utah

This project processes and analyses data on wildlife diseases using actual capture data from the State of Utah's Division of Wildlife Resources veterinarian office. This data is exceptionally expensive to collect, and I am deeply grateful to the Utah DWR for sharing such interesting real-world data with me for this project. 

This project also serves as the Capstone for the Udacity Data Scientist Nanodegree, and is graded according to [this rubric](https://learn.udacity.com/nanodegrees/nd025/parts/cd1971/lessons/c20e1b63-c711-475b-b1ba-3ea987081193/concepts/c20e1b63-c711-475b-b1ba-3ea987081193-project-rubric). 

## Project Definition

This project has the following parts:
1. An ETL pipeline (wildlife_ETL.py) which processes data from lab results. The pipelines have two outputs:
    - A set of excel files matching the format used by the Utah DWR vet office. 
    - A SQLite database for further analysis within this project. 
2. A Jupyter notebook (wildlife_disease.ipynb) analysing the data to gain insights, and consists of the following sections:
    - Problem statement
    - Data prepration
    - Analysis
    - Results
    - Conclusion
3. A [Medium blog post](https://medium.com/@wilson.adamp/a-look-at-wildlife-diseases-in-utah-a5a025f90545) written for a technical audience. 
4. The corresponding [github repo](https://github.com/epistemetrica/utah_dwr_python). 

## Motivation

As an avid outdoorsman, hiker, mountaineer, and big-game hunter, I have long had a passion for wildlife and wild places. And as it just so happens, a few years ago the State of Utah hired one of my oldest friends to be their wildlife veterinarian. 

I was intent on using the Udacity Data Scientist Capstone project to provide actual value, so I reached out to my friend to see if there was some data cleaning and analysis that I could do for her. This project is the result. 

## Data

The data come to me in two parts:
1. Excel sheets provided by the DWR Vet Office showing animal id, species, capture dates, biometrics, and locations. 
2. PDFs from various labs showing the results of various disease tests performed on each animal. Tables were extracted from these PDFs into excel documents using [Tabula](https://tabula.technology/)'s desktop app. (A more talented data scientist perhaps could have figued out a way to do this using the tabula-py library, but I could not.)

*NOTE: The data is available to Udacity reviewers and mentors on request, but will not be made public due to the extremely high cost of gathering the data.*

Please see the definitions.md file in the main project directory for the specifics of each variable.

## File Descriptions
- wildlife_ETL.py
    - the ETL pipeline that processes data extracted from lab results
- wildlife_disease.ipynb
    - the main Jupyter notebook with analysis, modeling, and results discussion. 
- definitions.md
    - a markdown file describing each variable used

## Libraries 

- pandas
- numpy
- re
- seaborn
- matplotlib
- scipy
- sqlachemy
- sklearn 
    - model_selection
    - pipeline
    - preprocessing
    - linear_model
    - neighbors
    - metrics

## Instructions

These instructions apply to *Udacity reviewers ONLY*. The data is not available for public use.

All data files were provided via the zip upload upon project submission. Please run wildlife_ETL.py before before running the main notebook (wildlife_disease.ipynb). Thanks! 

## Results Summary

### HD positivity and Pregnancy Rates:

The t-tests confirmed our suspicions: we cannot reject the null hypothesis that pregnancy rates are the same between females who test positive or negative for HD pathogens, so we cannot say from these data whether or not there is a relationship. It should be noted, however, that [many](https://www.ncbi.nlm.nih.gov/pmc/articles/PMC8402076/) [studies](https://pubmed.ncbi.nlm.nih.gov/8001344/) have been done on this topic and show that HD pathogens have a negative effect on reproductive health in many species, so the lack of evidence in this data set may simply be a  result of this being single-season cross-sectional observation data (for instance, we do not observe whether any of the pregnancies indicated in these data resulted in stillbirths, malformations, healthy deliveries and strong calf recruitment, etc etc).

### Predicting HD status of mule deer: 
<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>precision</th>
      <th>recall</th>
      <th>f1-score</th>
      <th>support</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>0.824561</td>
      <td>0.921569</td>
      <td>0.870370</td>
      <td>51.000000</td>
    </tr>
    <tr>
      <th>1</th>
      <td>0.666667</td>
      <td>0.444444</td>
      <td>0.533333</td>
      <td>18.000000</td>
    </tr>
    <tr>
      <th>accuracy</th>
      <td>0.797101</td>
      <td>0.797101</td>
      <td>0.797101</td>
      <td>0.797101</td>
    </tr>
    <tr>
      <th>macro avg</th>
      <td>0.745614</td>
      <td>0.683007</td>
      <td>0.701852</td>
      <td>69.000000</td>
    </tr>
    <tr>
      <th>weighted avg</th>
      <td>0.783371</td>
      <td>0.797101</td>
      <td>0.782448</td>
      <td>69.000000</td>
    </tr>
  </tbody>
</table>
</div>

With less than half of animals predicted to test positive actually testing positive (i.e., very high Type II error rate), it’s safe to say this model still errs strongly on the side of predicting negative results for HD pathogen tests. Given that 72% of the deer in our data tested negative, it seems the model is still leaning heavily toward the sample mean.

All in all, these data don’t seem to give us a reliable way to predict HD positivity. As an eager, wildlife-loving data scientist, my hopes were admittedly high going into this project, but this result is probably unsurprising to folks better educated on wildlife disease.

Conversations with the wildlife vet in Utah indicate that, while HD can be devastating in some populations, mule deer sometimes never even become symptomatic. It is also possible for HD to kill quickly, meaning the animal’s body condition does not deteriorate before death. In other words, it’s possible that some of the deer represented in our sample died of HD shortly after being captured, but their body conditions, pregnancy status, and other metrics would not be correlated to that potential.


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