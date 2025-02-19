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

# Key Features

### As a Tool - **Data Commander**:

-   A separate interface in the style of a commander.
-   Additional action settings that expand data interaction capabilities.
-   Hotkeys similar to Norton or other similar commanders.
-   A dedicated taskbar - everything is here and now.
-   Data structure is preserved.

### As a Service - **Move/Copy**:

-   Familiar Supervisely interface.
-   Works with data: projects, datasets, and items.
-   Simple copy and move actions without lengthy decision-making:
    -   Data and their annotations move together.
    -   If entities have matching names, they are not moved to avoid conflicts. For example, if an image with the same name already exists, neither it nor its annotation in the source project will overwrite the entity in the destination project.
-   Data structure is preserved.

### As a Service - **Transfer Annotated Items**:

-   Familiar Supervisely interface.
-   Copies items and their annotations from one location to another while maintaining structure.
-   Copying can be initiated from: projects, datasets, labeling jobs, and queues.
-   Copies only items that meet the following conditions:
    -   Were annotated within a Labeling Job, and these jobs are marked **as completed**.
    -   Were marked **as accepted** during review.
    -   Have no remaining labeling jobs with this items in an unfinished status.
-   Automatically resolves conflicts by renaming conflicting objects with a sequential index.

**Hierarchy of Transfers and Result:**

1. When initiating the process from the following instances, the system searches for related Labeling Jobs and exports items based on their results:

    - **Project → Workspace** → A new project is created, preserving the structure.
    - **Project → Project** → A dataset with the source project's name is created in the destination project, preserving nesting.
    - **Project → Dataset** → Similar to the previous point, but the dataset is nested inside the destination dataset.
    - **Dataset → Project** → A top-level dataset with the same name is created, containing only the items from that dataset (excluding nested datasets).
    - **Dataset → Dataset** → Similar to the previous point, but the dataset is nested inside the destination dataset.

2. When initiating the process from the following instances, processing is limited to the specified Labeling Jobs, and items are exported based on their results:
    - **Job** follows the same logic as **Dataset**. This means that if some items in an unrelated job have already been annotated and are in the required status, they will not be included.
    - **Queue** follows the same logic as **Job**, but considers all its jobs as the only recognized completed ones.

# How to use

### As a tool `Data Commander`:

1. Go to the `Data Commander`

<img width="70%" src="https://github.com/user-attachments/assets/aae1e64c-50b2-4baa-b17b-b81861bfd9bf" />

2. Navigate through the structures and select items

<img width="70%" src="https://github.com/user-attachments/assets/367fced3-6a53-41e7-bd16-259566f64c80" />

3. Click on the buttons to perform actions and select additional settings

<img width="70%" src="https://github.com/user-attachments/assets/e57153d2-16c4-4ab4-8584-76598cb3c918" />

4. Track the task progress in the progress bar

<img width="70%" src="https://github.com/user-attachments/assets/a28bb909-12fa-4f95-a817-78d852671456" />

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
