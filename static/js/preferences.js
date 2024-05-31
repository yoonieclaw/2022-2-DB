// preference.js

function setAttributes(element, attributes) {
    Object.keys(attributes).forEach((name) => {
        element.setAttribute(name, attributes[name]);
    });
}


// 대표 속성
async function createList(fetchURL,tbody) {
    let fetchedList = await fetch(fetchURL).then((res) => res.json());
    let tableBody = document.getElementById(tbody);
    
    for (const stdName of fetchedList) {
        let tableRow = document.createElement('tr');
        
        // 이름
        let tableDataName = document.createElement('td');
        let attrNameInput = document.createElement('input');
        setAttributes(attrNameInput, {
            class: 'name',
            name: stdName,
            type: 'text',
            form: 'edit',
            value: stdName,
            disabled: 'true',
            style: 'width: 100%;',
        });
        
        tableDataName.appendChild(attrNameInput);

        // 수정
        let tableDataEdit = document.createElement('td');

        let saveButton = document.createElement('button');
        saveButton.innerHTML = '저장';
        setAttributes(saveButton, {
            class: 'btn',
            form: 'edit',
            style: 'display: none;',
        });

        let editType = document.createElement('input');
        setAttributes(editType, {
            type: 'hidden',
            name: 'type',
            value: tbody === 'ratbody' ? 'ra' : 'jk',
            form: 'edit',
            disabled: 'true',
        })

        let editButton = document.createElement('button');
        editButton.innerHTML = '수정';
        editButton.onclick = () => {
            editType.disabled = false;
            attrNameInput.disabled = false;
            saveButton.setAttribute('style', 'background-color: #408558');
            editButton.setAttribute('style', 'display: none');
        }
        setAttributes(editButton, {
            class: 'btn',
            type: 'button',
            style: 'background-color: #316CF4',
        })

        tableDataEdit.appendChild(editType);
        tableDataEdit.appendChild(saveButton);
        tableDataEdit.appendChild(editButton);

        // 삭제
        let tableDataDelete = document.createElement('td');
        let deleteForm = document.createElement('form');
        setAttributes(deleteForm, {
            action: '/preferences/delete',
            method: 'post',
        });

        let deleteName = document.createElement('input');
        setAttributes(deleteName, {
            type: 'hidden',
            name: 'target_name',
            value: stdName,
        });

        let deleteType = document.createElement('input');
        setAttributes(deleteType, {
            type: 'hidden',
            name: 'type',
            value: tbody === 'ratbody' ? 'ra' : 'jk',
        })

        let deleteButton = document.createElement('button');
        deleteButton.innerHTML = '삭제';
        setAttributes(deleteButton, {
            class: 'btn',
            style: 'background-color: #CB444A',
        });

        deleteForm.appendChild(deleteName);
        deleteForm.appendChild(deleteType);
        deleteForm.appendChild(deleteButton);
        tableDataDelete.appendChild(deleteForm);

        tableRow.appendChild(tableDataName);
        tableRow.appendChild(tableDataEdit);
        tableRow.appendChild(tableDataDelete);

        tableBody.appendChild(tableRow);
    }
}

createList('/stdattr', 'ratbody');
createList('/stdkey', 'jktbody');
