<div align="center" markdown>

<img src="https://github.com/user-attachments/assets/09252852-3f49-4bad-b665-14e52458e4c0" style="width: 65%;"/>

# Data Commander

[![](https://img.shields.io/badge/supervisely-ecosystem-brightgreen)](https://ecosystem.supervisely.com/apps/supervisely-ecosystem/data-commander)
[![](https://img.shields.io/badge/slack-chat-green.svg?logo=slack)](https://supervisely.com/slack)
![GitHub release (latest SemVer)](https://img.shields.io/github/v/release/supervisely-ecosystem/data-commander)

</div>

# Overview

### Data Commander: Your Data Management Solution

Data Commander is a user-friendly tool designed to simplify data organization. It helps you efficiently manage your projects, datasets, and items through an intuitive interface, allowing for easy movement, copying, and processing of data.

### Data Commander as a Service: Streamlined Data Management and Transfer

Data Commander as a Service offers two main functionalities:

1. **Move/Copy:**
   Effortlessly move or copy projects, datasets, and items across different hierarchy levels, while preserving the structure and annotations. Destination project definitions are updated automatically.

2. **Transfer:**
   All annotated entities from `completed` Labeling Jobs with an `accepted` status are included in the transfer. During the process, entities are **copied** to the destination, and a new backup version of the destination project is created. The hierarchy is maintained where possible, making it easy to manage your data across various structures. The destination project definitions are also updated to reflect the changes.

# Key features

### As a tool `Data Commander`:

### As a service **Move/Copy:**

### As a service **Transfer Annotated Items:**

# How to use

### As a tool `Data Commander`:

1. Go to the `Data Commander`

2. Navigate through the structures and select items

3. Click on the buttons to perform actions

4. Track the task progress in the progress bar

### As a service **Move/Copy:**

1. In the `Projects` management section, select the projects,datasets or items and tick the required checkboxes.

2. A new button will appear in the header: `With N selected`.

3. Choose the `Move selected` action or `Copy selected`

4. A modal window will appear, allowing you to select the destination for the items. You can create a new project or dataset during this process. Select the desired destination and click the `Move` or `Copy` button.

### As a service **Transfer Annotated Items:**

1. In the `Projects` management section, select the projects or datasets and tick the required checkboxes.

2. A new button will appear in the header: `With N selected`.

3. Choose the `Transfer annotated items` action.

4. A modal window will appear, allowing you to select the destination for the items. You can create a new project or dataset during this process. Select the desired destination and click the `Transfer` button.
