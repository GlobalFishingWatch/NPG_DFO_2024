


## Structure and descriptions

To learn more about this structure and how it is meant to be used
throughout different stages of the project, see [this
presentation](https://docs.google.com/presentation/d/1E51s4VhcLzCwN_v_yeOpaGLtNEF5fcZanlO1lRsmGiw/edit?usp=sharing).

    |- README.md        <- Top-level README on how to use this repo
    |- data             <- Data files. Create subfolders as necessary.
    |- docs             <- Documentation.
    |- models           <- Trained models, model summaries, etc.
    |- notebooks        <- Jupyter notebooks and R markdown. Naming convention TBD. Use jupytext to convert
    |                      to .py before committing and do not commit the notebook itself. Suggest having
    |                      subfolders for different subsets of analysis. Could also have different
    |                      subfolders for each person if that makes more sense for your project/team.
    |- outputs          <- Models results, static reports, etc. Create  additional subfolders as necessary.
    |    |- figures     <- Create versioned figures folders if desired.
    |- queries          <- SQL files (.sql, .jinja2, etc). Create subfolders as necessary. When creating
    |                      new tables in BigQuery, be sure to use a schema with good field descriptions.
    |- scripts          <- Regular python and R files. These may use code from <module> but are not
    |                      notebooks. A good place for scripts that run data pipelines, train models, etc.
    |- .gitignore       <- Modify as necessary.
