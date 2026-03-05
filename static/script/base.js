const authModal = document.getElementById("auth-modal");
const loginForm = document.getElementById("auth-login");
const signupForm = document.getElementById("auth-signup");
const forgotForm = document.getElementById("auth-forgot");
const authTitle = document.getElementById("auth-title");
const authSubtitle = document.getElementById("auth-subtitle");
const userSection = document.getElementById("user-section");
const authSection = document.getElementById("auth-section");
const sidebarUserAvatarInitial = document.getElementById("sidebar-user-avatar-initial");
const sidebarUserName = document.getElementById("sidebar-user-name");
const sidebarUserEmail = document.getElementById("sidebar-user-email");
const userMenuToggle = document.getElementById("user-menu-toggle");
const userMenuDropdown = document.getElementById("user-menu-dropdown");
const userMenuCaret = document.getElementById("user-menu-caret");
const userEditProfileBtn = document.getElementById("user-edit-profile-btn");
const userLogoutBtn = document.getElementById("user-logout-btn");
const emailVerifyBadge = document.getElementById("email-verify-badge");
const emailVerifyBanner = document.getElementById("email-verify-banner");
const emailVerifyResendBtn = document.getElementById("email-verify-resend-btn");
const emailVerifyResendStatus = document.getElementById("email-verify-resend-status");
const passwordResetBanner = document.getElementById("password-reset-banner");
const passwordResetCloseBtn = document.getElementById("password-reset-close-btn");
const passwordResetChangeBtn = document.getElementById("password-reset-change-btn");
const emailVerifyResultModal = document.getElementById("email-verify-result-modal");
const emailVerifyResultTitle = document.getElementById("email-verify-result-title");
const emailVerifyResultText = document.getElementById("email-verify-result-text");
const emailVerifyResultIcon = document.getElementById("email-verify-result-icon");
const emailVerifyResultBtn = document.getElementById("email-verify-result-btn");
const signupSubmitBtn = document.getElementById("signup-submit-btn");
const signupSubmitText = document.getElementById("signup-submit-text");
const signupSubmitSpinner = document.getElementById("signup-submit-spinner");
let currentAuthUser = null;
let isUserMenuOpen = false;

// ========== TOAST NOTIFICATIONS ==========
document.addEventListener("DOMContentLoaded", () => {
    const message = sessionStorage.getItem("toastMessage");
    if (!message) return;

    const toast = document.getElementById("toast-container");
    toast.textContent = message;
    toast.classList.remove("hidden");

    sessionStorage.removeItem("toastMessage");

    setTimeout(() => {
        toast.classList.add("hidden");
    }, 3000);
});

// ========== CUSTOM PLACEHOLDER MANAGEMENT ==========
const initCustomPlaceholders = () => {
    const inputs = document.querySelectorAll('#auth-modal input[type="text"], #auth-modal input[type="email"], #auth-modal input[type="password"]');
    
    inputs.forEach(input => {
        const container = input.parentElement;
        const placeholder = container.querySelector('.custom-placeholder');
        
        if (!placeholder) return;
        
        const updatePlaceholder = () => {
            if (input.value.length > 0) {
                placeholder.style.opacity = '0';
                placeholder.style.visibility = 'hidden';
            } else {
                placeholder.style.opacity = '1';
                placeholder.style.visibility = 'visible';
            }
        };
        
        input.addEventListener('focus', () => {
            placeholder.style.opacity = '0';
            placeholder.style.visibility = 'hidden';
        });
        
        input.addEventListener('blur', () => {
            if (input.value.length === 0) {
                placeholder.style.opacity = '1';
                placeholder.style.visibility = 'visible';
            }
        });
        
        input.addEventListener('input', updatePlaceholder);
        updatePlaceholder();
    });
};

if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initCustomPlaceholders);
} else {
    initCustomPlaceholders();
}

// ========== FORM UTILITIES ==========
const clearFormErrors = (formId) => {
    document.querySelectorAll(`#${formId} span[id$='-error']`).forEach((el) => {
        el.classList.add("hidden");
    });
    document.querySelectorAll(`#${formId} input`).forEach((input) => {
        input.classList.remove("auth-input-error");
    });
};

const resetAuthFormState = (formId) => {
    const form = document.getElementById(formId);
    if (!form) return;
    form.reset();
    clearFormErrors(formId);
    
    form.querySelectorAll("input[type='text']").forEach((input) => {
        if (input.id.toLowerCase().includes("password")) {
            input.type = "password";
        }
    });
    
    form.querySelectorAll('.custom-placeholder').forEach(placeholder => {
        placeholder.style.opacity = '1';
        placeholder.style.visibility = 'visible';
    });
};

const resetPasswordToggleIcons = () => {
    document.querySelectorAll(".auth-password-toggle").forEach((button) => {
        button.innerHTML = '<i class="fa-solid fa-eye"></i>';
        button.setAttribute("aria-label", "Show password");
    });
};

const resetAuthForms = () => {
    resetAuthFormState("auth-login");
    resetAuthFormState("auth-signup");
    resetAuthFormState("auth-forgot");
    resetPasswordToggleIcons();
    
    // Reset password strength dropdown
    const dropdown = document.getElementById('password-strength-dropdown');
    if (dropdown) {
        dropdown.classList.add('hidden');
        resetPasswordStrengthIndicators();
    }

    resetForgotPasswordStep();
    setSignupSubmitLoading(false);
};

const setSignupSubmitLoading = (isLoading) => {
    if (!signupSubmitBtn || !signupSubmitText || !signupSubmitSpinner) return;
    signupSubmitBtn.disabled = isLoading;
    signupSubmitBtn.setAttribute("aria-busy", isLoading ? "true" : "false");
    signupSubmitText.textContent = isLoading ? "Signing you up..." : "Sign Up";
    signupSubmitSpinner.classList.toggle("hidden", !isLoading);
};

const setEmailVerifyResendStatus = (message, isError = false) => {
    if (!emailVerifyResendStatus) return;
    emailVerifyResendStatus.textContent = message || "";
    emailVerifyResendStatus.classList.remove("text-emerald-700", "text-red-700");
    if (message) {
        emailVerifyResendStatus.classList.add(isError ? "text-red-700" : "text-emerald-700");
    }
};

const setUserMenuOpen = (open) => {
    if (!userMenuToggle || !userMenuDropdown) return;
    isUserMenuOpen = open;
    userMenuDropdown.classList.toggle("hidden", !open);
    userMenuToggle.setAttribute("aria-expanded", open ? "true" : "false");
    if (userMenuCaret) {
        userMenuCaret.classList.toggle("rotate-180", open);
    }
};

// ========== AUTH MODAL CONTROLS ==========
const setAuthMode = (mode) => {
    resetAuthForms();
    const isLogin = mode === "login";
    const isSignup = mode === "signup";
    const isForgot = mode === "forgot";

    loginForm.classList.toggle("hidden", !isLogin);
    signupForm.classList.toggle("hidden", !isSignup);
    forgotForm.classList.toggle("hidden", !isForgot);

    if (isLogin) {
        authTitle.textContent = "Welcome Back";
        authSubtitle.textContent = "Log in to continue";
    } else if (isSignup) {
        authTitle.textContent = "Create Your Account";
        authSubtitle.textContent = "Sign up in seconds";
    } else {
        authTitle.textContent = "Forgot Password";
        authSubtitle.textContent = "Enter your email to receive a reset link";
    }

    setTimeout(initCustomPlaceholders, 10);
};

const openAuth = (mode) => {
    setAuthMode(mode);
    authModal.classList.remove("hidden");
    authModal.classList.add("flex");
};

const closeAuth = () => {
    resetAuthForms();
    authModal.classList.add("hidden");
    authModal.classList.remove("flex");
};

document.querySelectorAll("[data-auth-open]").forEach((el) => {
    el.addEventListener("click", () => openAuth(el.dataset.authOpen));
});

document.querySelectorAll("[data-auth-close]").forEach((el) => {
    el.addEventListener("click", closeAuth);
});

document.addEventListener("keydown", (e) => {
    if (e.key === "Escape") closeAuth();
    if (e.key === "Escape") setUserMenuOpen(false);
});

const openEmailVerifyResultModal = (status) => {
    if (!emailVerifyResultModal) return;

    if (status === "success") {
        emailVerifyResultTitle.textContent = "Email Verified";
        emailVerifyResultText.textContent = "Congratulations, your email has been verified. Please enjoy InsightHub.";
        emailVerifyResultIcon.className = "fa-solid fa-circle-check text-2xl";
        emailVerifyResultIcon.parentElement.className =
            "h-14 w-14 rounded-full bg-emerald-100 text-emerald-700 flex items-center justify-center mx-auto mb-4";
    } else if (status === "invalid") {
        emailVerifyResultTitle.textContent = "Link Expired";
        emailVerifyResultText.textContent = "This verification link is invalid or expired. Please request a new verification email.";
        emailVerifyResultIcon.className = "fa-solid fa-link-slash text-2xl";
        emailVerifyResultIcon.parentElement.className =
            "h-14 w-14 rounded-full bg-amber-100 text-amber-700 flex items-center justify-center mx-auto mb-4";
    } else {
        emailVerifyResultTitle.textContent = "Verification Failed";
        emailVerifyResultText.textContent = "We could not verify your email right now. Please try again later.";
        emailVerifyResultIcon.className = "fa-solid fa-triangle-exclamation text-2xl";
        emailVerifyResultIcon.parentElement.className =
            "h-14 w-14 rounded-full bg-red-100 text-red-700 flex items-center justify-center mx-auto mb-4";
    }

    emailVerifyResultModal.classList.remove("hidden");
    emailVerifyResultModal.classList.add("flex");
};

const closeEmailVerifyResultModal = () => {
    if (!emailVerifyResultModal) return;
    emailVerifyResultModal.classList.add("hidden");
    emailVerifyResultModal.classList.remove("flex");
};

document.querySelectorAll("[data-email-verify-close]").forEach((el) => {
    el.addEventListener("click", closeEmailVerifyResultModal);
});
if (emailVerifyResultBtn) {
    emailVerifyResultBtn.addEventListener("click", closeEmailVerifyResultModal);
}
document.addEventListener("keydown", (e) => {
    if (e.key === "Escape") closeEmailVerifyResultModal();
    if (e.key === "Escape") setUserMenuOpen(false);
});

// ========== VALIDATION ==========
const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
const strongPasswordRegex = /^(?=.*[a-z])(?=.*[A-Z])(?=.*\d)(?=.*[^A-Za-z\d]).{8,}$/;

const showError = (input, errorId, show) => {
    const error = document.getElementById(errorId);
    if (!error) return;
    error.classList.toggle("hidden", !show);
    if (input) {
        input.classList.toggle("auth-input-error", show);
    }
};

// ========== FORGOT PASSWORD FLOW ==========
const forgotEmailInput = document.getElementById("forgot-email");
const forgotSubmitBtn = document.getElementById("forgot-submit-btn");
const forgotSuccess = document.getElementById("forgot-success");

const setAuthUiState = (user) => {
    currentAuthUser = user || null;
    const isLoggedIn = Boolean(user && user.user_id);
    userSection.classList.toggle("hidden", !isLoggedIn);
    authSection.classList.toggle("hidden", isLoggedIn);

    if (!isLoggedIn) {
        setUserMenuOpen(false);
        if (emailVerifyBanner) emailVerifyBanner.classList.add("hidden");
        if (emailVerifyBadge) emailVerifyBadge.classList.add("hidden");
        setEmailVerifyResendStatus("");
        return;
    }

    const username = (user.username || "").trim();
    const email = (user.email || "").trim();
    const initial = username ? username.charAt(0).toUpperCase() : "U";
    const isVerified = Boolean(user.email_verified);

    if (sidebarUserAvatarInitial) sidebarUserAvatarInitial.textContent = initial;
    sidebarUserName.textContent = username || "User";
    sidebarUserEmail.textContent = email;
    if (emailVerifyBanner) emailVerifyBanner.classList.toggle("hidden", isVerified);
    if (emailVerifyBadge) emailVerifyBadge.classList.toggle("hidden", isVerified);
    if (isVerified) setEmailVerifyResendStatus("");
};

if (userMenuToggle && userMenuDropdown) {
    userMenuToggle.addEventListener("click", () => {
        setUserMenuOpen(!isUserMenuOpen);
    });

    document.addEventListener("click", (e) => {
        if (!isUserMenuOpen) return;
        if (userSection && !userSection.contains(e.target)) {
            setUserMenuOpen(false);
        }
    });
}

if (userEditProfileBtn) {
    userEditProfileBtn.addEventListener("click", async () => {
        if (!currentAuthUser) return;
        const currentName = (currentAuthUser.username || "").trim();
        const nextName = (window.prompt("Enter your new display name:", currentName) || "").trim();

        if (!nextName || nextName === currentName) {
            setUserMenuOpen(false);
            return;
        }

        try {
            const response = await fetch("/api/auth/profile", {
                method: "POST",
                headers: {
                    "Content-Type": "application/json",
                },
                body: JSON.stringify({ username: nextName }),
            });
            const payload = await response.json();
            if (!response.ok) {
                window.alert(payload.error || "Unable to update profile right now.");
                return;
            }
            setAuthUiState(payload.user);
            window.alert("Profile updated.");
        } catch (error) {
            window.alert("Network error. Please try again.");
        } finally {
            setUserMenuOpen(false);
        }
    });
}

if (userLogoutBtn) {
    userLogoutBtn.addEventListener("click", async () => {
        try {
            const response = await fetch("/api/auth/logout", {
                method: "POST",
                headers: {
                    "Content-Type": "application/json",
                },
            });
            const payload = await response.json();
            if (!response.ok) {
                window.alert(payload.error || "Unable to log out right now.");
                return;
            }

            setAuthUiState(null);
            setUserMenuOpen(false);

            sessionStorage.setItem("toastMessage", "You have logged out successfully.");

            window.location.href = "/dashboard";
        } catch (error) {
            window.alert("Network error. Please try again.");
        }
    });
}

if (emailVerifyResendBtn) {
    emailVerifyResendBtn.addEventListener("click", async () => {
        if (!currentAuthUser || currentAuthUser.email_verified) return;

        emailVerifyResendBtn.disabled = true;
        setEmailVerifyResendStatus("Sending...");

        try {
            const response = await fetch("/api/auth/resend-verification", {
                method: "POST",
                headers: {
                    "Content-Type": "application/json",
                },
            });
            const payload = await response.json();
            if (!response.ok) {
                setEmailVerifyResendStatus(payload.error || "Failed to resend email.", true);
                return;
            }
            setEmailVerifyResendStatus(payload.message || "Verification email sent.");
        } catch (error) {
            setEmailVerifyResendStatus("Network error. Please try again.", true);
        } finally {
            emailVerifyResendBtn.disabled = false;
        }
    });
}

const resetForgotPasswordStep = () => {
    if (forgotSubmitBtn) forgotSubmitBtn.textContent = "Send Reset Link";
    if (forgotSuccess) forgotSuccess.classList.add("hidden");
};

const hideAllErrorsForInput = (input) => {
    const inputId = input.id;
    document.querySelectorAll(`span[id^="${inputId}-"]`).forEach(error => {
        error.classList.add("hidden");
    });
    input.classList.remove("auth-input-error");
};

// ========== PASSWORD STRENGTH INDICATOR ==========
const passwordStrengthDropdown = document.getElementById('password-strength-dropdown');
const signupPasswordInput = document.getElementById('signup-password');
const signupPasswordField = signupPasswordInput ? signupPasswordInput.closest('.relative') : null;

const updatePasswordStrengthIndicator = (requirement, isValid) => {
    const element = document.getElementById(requirement);
    if (!element) return;
    
    const icon = element.querySelector('i');
    const text = element.querySelector('span');
    
    if (isValid) {
        icon.classList.remove('fa-xmark', 'text-red-500');
        icon.classList.add('fa-check', 'text-green-500');
        text.classList.remove('text-slate-500');
        text.classList.add('text-green-600');
    } else {
        icon.classList.remove('fa-check', 'text-green-500');
        icon.classList.add('fa-xmark', 'text-red-500');
        text.classList.remove('text-green-600');
        text.classList.add('text-slate-500');
    }
};

const resetPasswordStrengthIndicators = () => {
    ['pwd-length', 'pwd-uppercase', 'pwd-lowercase', 'pwd-number', 'pwd-special'].forEach(req => {
        updatePasswordStrengthIndicator(req, false);
    });
};

const checkPasswordStrength = (password) => {
    const hasLength = password.length >= 8;
    const hasUppercase = /[A-Z]/.test(password);
    const hasLowercase = /[a-z]/.test(password);
    const hasNumber = /\d/.test(password);
    const hasSpecial = /[^A-Za-z\d]/.test(password);
    
    updatePasswordStrengthIndicator('pwd-length', hasLength);
    updatePasswordStrengthIndicator('pwd-uppercase', hasUppercase);
    updatePasswordStrengthIndicator('pwd-lowercase', hasLowercase);
    updatePasswordStrengthIndicator('pwd-number', hasNumber);
    updatePasswordStrengthIndicator('pwd-special', hasSpecial);
    
    return hasLength && hasUppercase && hasLowercase && hasNumber && hasSpecial;
};

// Show/hide dropdown based on hover, not focus.
if (signupPasswordField && passwordStrengthDropdown) {
    signupPasswordField.addEventListener('mouseenter', () => {
        passwordStrengthDropdown.classList.remove('hidden');
        hideAllErrorsForInput(signupPasswordInput);
    });

    signupPasswordField.addEventListener('mouseleave', () => {
        passwordStrengthDropdown.classList.add('hidden');
    });
}

// ========== PASSWORD TOGGLE ==========
document.querySelectorAll(".auth-password-toggle").forEach((button) => {
    button.addEventListener("click", () => {
        const input = document.getElementById(button.dataset.target);
        if (!input) return;

        const isHidden = input.type === "password";
        input.type = isHidden ? "text" : "password";
        button.innerHTML = isHidden
            ? '<i class="fa-solid fa-eye-slash"></i>'
            : '<i class="fa-solid fa-eye"></i>';
        button.setAttribute("aria-label", isHidden ? "Hide password" : "Show password");
    });
});

// ========== REAL-TIME VALIDATION - LOGIN ==========
const loginEmailInput = document.getElementById("login-email");
const loginPasswordInput = document.getElementById("login-password");

loginEmailInput.addEventListener('focus', () => {
    hideAllErrorsForInput(loginEmailInput);
});

loginPasswordInput.addEventListener('focus', () => {
    hideAllErrorsForInput(loginPasswordInput);
});

loginEmailInput.addEventListener('input', () => {
    const email = loginEmailInput.value.trim();
    
    if (email.length === 0) {
        hideAllErrorsForInput(loginEmailInput);
    } else if (!emailRegex.test(email)) {
        showError(loginEmailInput, "login-email-error", true);
        document.getElementById("login-email-error").textContent = "Please enter a valid email.";
    } else {
        hideAllErrorsForInput(loginEmailInput);
    }
});

loginPasswordInput.addEventListener('input', () => {
    const password = loginPasswordInput.value;
    
    if (password.length === 0) {
        hideAllErrorsForInput(loginPasswordInput);
    } else {
        hideAllErrorsForInput(loginPasswordInput);
    }
});

// ========== REAL-TIME VALIDATION - SIGNUP ==========
const signupNameInput = document.getElementById("signup-name");
const signupEmailInput = document.getElementById("signup-email");
const signupConfirmPasswordInput = document.getElementById("signup-confirm-password");

signupNameInput.addEventListener('focus', () => {
    hideAllErrorsForInput(signupNameInput);
});

signupEmailInput.addEventListener('focus', () => {
    hideAllErrorsForInput(signupEmailInput);
});

signupConfirmPasswordInput.addEventListener('focus', () => {
    hideAllErrorsForInput(signupConfirmPasswordInput);
});

signupNameInput.addEventListener('input', () => {
    const name = signupNameInput.value.trim();
    
    if (name.length === 0) {
        hideAllErrorsForInput(signupNameInput);
    } else {
        hideAllErrorsForInput(signupNameInput);
    }
});

signupEmailInput.addEventListener('input', () => {
    const email = signupEmailInput.value.trim();
    
    if (email.length === 0) {
        hideAllErrorsForInput(signupEmailInput);
    } else if (!emailRegex.test(email)) {
        showError(signupEmailInput, "signup-email-error", true);
        document.getElementById("signup-email-error").textContent = "Please enter a valid email.";
    } else {
        hideAllErrorsForInput(signupEmailInput);
    }
});

// Password validation with strength indicator
signupPasswordInput.addEventListener('input', () => {
    const password = signupPasswordInput.value;
    const confirmPassword = signupConfirmPasswordInput.value;
    
    if (password.length === 0) {
        hideAllErrorsForInput(signupPasswordInput);
        resetPasswordStrengthIndicators();
    } else {
        const isStrong = checkPasswordStrength(password);
        
        // Auto-hide dropdown when all requirements are met
        if (isStrong) {
            setTimeout(() => {
                if (document.activeElement !== signupPasswordInput) {
                    passwordStrengthDropdown.classList.add('hidden');
                }
            }, 1000);
        }
        
        hideAllErrorsForInput(signupPasswordInput);
    }
    
    // Also validate confirm password if it has content
    if (confirmPassword.length > 0) {
        if (password !== confirmPassword) {
            showError(signupConfirmPasswordInput, "signup-password-match-error", true);
        } else {
            hideAllErrorsForInput(signupConfirmPasswordInput);
        }
    }
});

signupConfirmPasswordInput.addEventListener('input', () => {
    const password = signupPasswordInput.value;
    const confirmPassword = signupConfirmPasswordInput.value;
    
    if (confirmPassword.length === 0) {
        hideAllErrorsForInput(signupConfirmPasswordInput);
    } else if (password !== confirmPassword) {
        showError(signupConfirmPasswordInput, "signup-password-match-error", true);
    } else {
        hideAllErrorsForInput(signupConfirmPasswordInput);
    }
});

// ========== REAL-TIME VALIDATION - FORGOT PASSWORD ==========
forgotEmailInput.addEventListener('focus', () => {
    hideAllErrorsForInput(forgotEmailInput);
});

forgotEmailInput.addEventListener('input', () => {
    const email = forgotEmailInput.value.trim();

    if (email.length === 0) {
        hideAllErrorsForInput(forgotEmailInput);
    } else if (!emailRegex.test(email)) {
        showError(forgotEmailInput, "forgot-email-error", true);
        document.getElementById("forgot-email-error").textContent = "Please enter a valid email.";
    } else {
        hideAllErrorsForInput(forgotEmailInput);
    }
});

// ========== LOGIN FORM SUBMISSION ==========
loginForm.addEventListener("submit", async (e) => {
    e.preventDefault();
    clearFormErrors("auth-login");

    const email = loginEmailInput.value.trim();
    const password = loginPasswordInput.value;
    let isValid = true;

    if (!email) {
        showError(loginEmailInput, "login-email-error", true);
        document.getElementById("login-email-error").textContent = "Please enter your email.";
        isValid = false;
    } else if (!emailRegex.test(email)) {
        showError(loginEmailInput, "login-email-error", true);
        document.getElementById("login-email-error").textContent = "Please enter a valid email.";
        isValid = false;
    }

    if (!password.trim()) {
        showError(loginPasswordInput, "login-password-error", true);
        isValid = false;
    }

    if (!isValid) return;

    try {
        const response = await fetch("/api/auth/login", {
            method: "POST",
            headers: {
                "Content-Type": "application/json",
            },
            body: JSON.stringify({
                email,
                password,
            }),
        });

        const payload = await response.json();
        if (!response.ok) {
            if (response.status === 401) {
                showError(loginPasswordInput, "login-password-error", true);
                document.getElementById("login-password-error").textContent = payload.error || "Invalid email or password.";
                return;
            }

            showError(loginEmailInput, "login-email-error", true);
            document.getElementById("login-email-error").textContent = payload.error || "Login failed. Please try again.";
            return;
        }

        setAuthUiState({
            ...payload.user,
            email_verified: Boolean(payload.user?.email_verified),
        });
        closeAuth();
    } catch (error) {
        showError(loginEmailInput, "login-email-error", true);
        document.getElementById("login-email-error").textContent = "Network error. Please try again.";
    }
});

// ========== SIGNUP FORM SUBMISSION ==========
signupForm.addEventListener("submit", async (e) => {
    e.preventDefault();
    clearFormErrors("auth-signup");

    const name = signupNameInput.value.trim();
    const email = signupEmailInput.value.trim();
    const password = signupPasswordInput.value;
    const confirmPassword = signupConfirmPasswordInput.value;
    let isValid = true;

    if (!name) {
        showError(signupNameInput, "signup-name-error", true);
        isValid = false;
    }

    if (!email) {
        showError(signupEmailInput, "signup-email-error", true);
        document.getElementById("signup-email-error").textContent = "Please enter your email.";
        isValid = false;
    } else if (!emailRegex.test(email)) {
        showError(signupEmailInput, "signup-email-error", true);
        document.getElementById("signup-email-error").textContent = "Please enter a valid email.";
        isValid = false;
    }

    if (!password.trim()) {
        showError(signupPasswordInput, "signup-password-error", true);
        isValid = false;
    } else if (!strongPasswordRegex.test(password)) {
        showError(signupPasswordInput, "signup-password-strength-error", true);
        isValid = false;
    }

    if (!confirmPassword.trim()) {
        showError(signupConfirmPasswordInput, "signup-confirm-password-error", true);
        isValid = false;
    } else if (password !== confirmPassword) {
        showError(signupConfirmPasswordInput, "signup-password-match-error", true);
        isValid = false;
    }

    if (!isValid) return;

    setSignupSubmitLoading(true);
    try {
        const response = await fetch("/api/auth/signup", {
            method: "POST",
            headers: {
                "Content-Type": "application/json",
            },
            body: JSON.stringify({
                username: name,
                email,
                password,
            }),
        });

        const payload = await response.json();
        if (!response.ok) {
            if (response.status === 409) {
                showError(signupEmailInput, "signup-email-error", true);
                document.getElementById("signup-email-error").textContent = payload.error || "An account with this email already exists.";
                return;
            }

            showError(signupEmailInput, "signup-email-error", true);
            document.getElementById("signup-email-error").textContent = payload.error || "Signup failed. Please try again.";
            return;
        }

        console.log("Signup successful:", payload.user);
        setAuthUiState({
            ...payload.user,
            email_verified: Boolean(payload.user?.email_verified),
        });
        closeAuth();
    } catch (error) {
        showError(signupEmailInput, "signup-email-error", true);
        document.getElementById("signup-email-error").textContent = "Network error. Please try again.";
    } finally {
        setSignupSubmitLoading(false);
    }
});

// ========== FORGOT PASSWORD FORM SUBMISSION ==========
forgotForm.addEventListener("submit", async (e) => {
    e.preventDefault();
    clearFormErrors("auth-forgot");
    if (forgotSuccess) forgotSuccess.classList.add("hidden");

    const email = forgotEmailInput.value.trim();
    let isValid = true;

    if (!email) {
        showError(forgotEmailInput, "forgot-email-error", true);
        document.getElementById("forgot-email-error").textContent = "Please enter your email.";
        isValid = false;
    } else if (!emailRegex.test(email)) {
        showError(forgotEmailInput, "forgot-email-error", true);
        document.getElementById("forgot-email-error").textContent = "Please enter a valid email.";
        isValid = false;
    }

    if (!isValid) return;

    try {
        const response = await fetch("/api/auth/forgot-password/request", {
            method: "POST",
            headers: {
                "Content-Type": "application/json",
            },
            body: JSON.stringify({ email }),
        });
        const payload = await response.json();

        if (!response.ok) {
            showError(forgotEmailInput, "forgot-email-error", true);
            document.getElementById("forgot-email-error").textContent =
                payload.error || "Unable to send reset link. Please try again.";
            return;
        }

        if (forgotSuccess) forgotSuccess.classList.remove("hidden");
        if (forgotSubmitBtn) forgotSubmitBtn.textContent = "Resend Reset Link";
    } catch (error) {
        showError(forgotEmailInput, "forgot-email-error", true);
        document.getElementById("forgot-email-error").textContent =
            "Network error. Please try again.";
    }
});

const SIDEBAR_STATE_KEY = "insighthub.sidebarCollapsed";
const CONVO_STATE_KEY = "insighthub.convoOpen";
const THEME_STATE_KEY = "insighthub.theme";

// Sidebar toggle
const sidebarToggle = document.getElementById("sidebar-toggle");

// Conversations submenu toggle
const convoToggle = document.getElementById("convo-toggle");
const convoSubmenu = document.getElementById("convo-submenu");
const convoCaret = document.getElementById("convo-caret");
const themeLightRadio = document.getElementById("theme-light");
const themeDarkRadio = document.getElementById("theme-dark");
const themeLightOption = document.getElementById("theme-light-option");
const themeDarkOption = document.getElementById("theme-dark-option");

const syncConvoCaret = () => {
    const isOpen = !convoSubmenu.classList.contains("hidden");
    convoCaret.classList.toggle("rotate-180", isOpen);
};

const syncThemeOptions = (theme) => {
    const isDark = theme === "dark";
    themeLightOption.classList.toggle("active", !isDark);
    themeDarkOption.classList.toggle("active", isDark);
    themeLightRadio.checked = !isDark;
    themeDarkRadio.checked = isDark;
};

const applyTheme = (theme, persist = true) => {
    const nextTheme = theme === "dark" ? "dark" : "light";
    document.body.classList.toggle("theme-dark", nextTheme === "dark");
    document.body.classList.toggle("theme-light", nextTheme !== "dark");
    syncThemeOptions(nextTheme);
    if (persist) {
        localStorage.setItem(THEME_STATE_KEY, nextTheme);
    }
};

// Restore persisted UI state on refresh
const isSidebarCollapsed = localStorage.getItem(SIDEBAR_STATE_KEY) === "true";
if (isSidebarCollapsed) {
    document.body.classList.add("sidebar-collapsed");
}

const savedConvoOpen = localStorage.getItem(CONVO_STATE_KEY);
if (savedConvoOpen === "false") {
    convoSubmenu.classList.add("hidden");
} else if (savedConvoOpen === "true") {
    convoSubmenu.classList.remove("hidden");
}

const savedTheme = localStorage.getItem(THEME_STATE_KEY);
applyTheme(savedTheme === "dark" ? "dark" : "light", false);
syncConvoCaret();

const params = new URLSearchParams(window.location.search);
const emailVerifiedStatus = params.get("email_verified");
const passwordResetStatus = params.get("pwd_reset");
if (emailVerifiedStatus) {
    openEmailVerifyResultModal(emailVerifiedStatus);
    params.delete("email_verified");
}
if (passwordResetStatus === "success" && passwordResetBanner) {
    passwordResetBanner.classList.remove("hidden");
}
params.delete("pwd_reset");
const query = params.toString();
const nextUrl = `${window.location.pathname}${query ? `?${query}` : ""}${window.location.hash}`;
window.history.replaceState({}, "", nextUrl);

if (passwordResetCloseBtn && passwordResetBanner) {
    passwordResetCloseBtn.addEventListener("click", () => {
        passwordResetBanner.classList.add("hidden");
    });
}

if (passwordResetChangeBtn) {
    passwordResetChangeBtn.addEventListener("click", async () => {
        const newPassword = window.prompt("Enter your new password:");
        if (!newPassword) return;

        if (!strongPasswordRegex.test(newPassword)) {
            window.alert("Password must be at least 8 characters and include uppercase, lowercase, number, and special character.");
            return;
        }

        const confirmPassword = window.prompt("Confirm your new password:");
        if (newPassword !== confirmPassword) {
            window.alert("Passwords do not match.");
            return;
        }

        try {
            const response = await fetch("/api/auth/change-password", {
                method: "POST",
                headers: {
                    "Content-Type": "application/json",
                },
                body: JSON.stringify({ new_password: newPassword }),
            });
            const payload = await response.json();
            if (!response.ok) {
                window.alert(payload.error || "Unable to change password right now.");
                return;
            }

            if (passwordResetBanner) passwordResetBanner.classList.add("hidden");
            window.alert("Password changed successfully.");
        } catch (error) {
            window.alert("Network error. Please try again.");
        }
    });
}

sidebarToggle.addEventListener("click", () => {
    setUserMenuOpen(false);
    document.body.classList.toggle("sidebar-collapsed");
    localStorage.setItem(
        SIDEBAR_STATE_KEY,
        document.body.classList.contains("sidebar-collapsed")
    );
});

convoToggle.addEventListener("click", () => {
    const isHidden = convoSubmenu.classList.toggle("hidden");
    localStorage.setItem(CONVO_STATE_KEY, (!isHidden).toString());
    syncConvoCaret();
});

themeLightRadio.addEventListener("change", () => {
    if (themeLightRadio.checked) applyTheme("light");
});
themeDarkRadio.addEventListener("change", () => {
    if (themeDarkRadio.checked) applyTheme("dark");
});

setAuthUiState(window.__AUTH_USER__ || null);
